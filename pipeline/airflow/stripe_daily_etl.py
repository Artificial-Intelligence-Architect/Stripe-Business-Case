"""
stripe_daily_etl.py
===================
Daily orchestration: source validation → dbt → freshness assertion →
GDPR/CCPA compliance report.

FIXES APPLIED vs v1
-------------------
1. `validate_sources` was `SELECT COUNT(*)` with no assertion: the task went
   green on an empty load. A validation that cannot fail is not a validation.
   It now raises on zero rows and on row-count collapse vs the trailing median.

2. `generate_gdpr_report` counted erasures with `operation = 'U'` while
   gdpr_erase_customer() wrote `'D'` → the metric was hard-wired to 0.
   Both sides now agree on `operation = 'U'` + `reason` column.

3. `refresh_views` issued `ALTER DYNAMIC TABLE ... REFRESH` on tables that
   already declare TARGET_LAG. Declaring a lag AND forcing a refresh are two
   contradictory freshness contracts, and the manual one silently doubles the
   warehouse bill. Replaced by an ASSERTION that the declared lag is met.

4. The GDPR SLA check ran AFTER the dbt tasks, so a dbt failure meant the
   compliance report never ran — the legal deadline was coupled to the
   analytics pipeline. Compliance now runs on an independent branch.
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import json

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=45),
    "email_on_failure": True,
    "email_on_retry": False,
}

# Freshness contract — must match TARGET_LAG in sql/olap/schema.sql.
# Single source of truth; drift between the two is what this dict prevents.
DYNAMIC_TABLE_LAG_MINUTES = {
    "mv_daily_revenue": 60,
    "mv_customer_monthly": 1440,
    "mv_subscription_mrr": 60,
}


def validate_sources(**context):
    """Assert the raw layer actually received yesterday's data.

    Two independent failure modes are checked:
      - hard zero  : the Kafka→S3→Snowpipe chain is broken
      - collapse   : data arrived but volume is anomalously low (partial load)
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    sf = SnowflakeHook(snowflake_conn_id="snowflake_prod")
    ds = context["ds"]

    row_count = sf.get_first(
        "SELECT COUNT(*) FROM staging.raw_transactions WHERE load_date = %s",
        parameters=(ds,),
    )[0]

    if row_count == 0:
        raise AirflowException(
            f"No rows in staging.raw_transactions for {ds}. "
            "Ingestion chain (Debezium → Kafka → S3 → Snowpipe) is broken. "
            "Runbook: docs/10_runbooks.md#ingestion-gap"
        )

    # Volume regression: compare against the median of the last 14 days.
    median_14d = sf.get_first(
        """
        SELECT MEDIAN(daily_count) FROM (
            SELECT load_date, COUNT(*) AS daily_count
            FROM staging.raw_transactions
            WHERE load_date BETWEEN DATEADD('day', -14, %s) AND DATEADD('day', -1, %s)
            GROUP BY load_date
        )
        """,
        parameters=(ds, ds),
    )[0]

    if median_14d and row_count < median_14d * 0.5:
        raise AirflowException(
            f"Volume collapse on {ds}: {row_count:,} rows vs 14-day median "
            f"{median_14d:,.0f} (-{100 * (1 - row_count / median_14d):.0f}%). "
            "Likely a partial load. Refusing to publish to marts."
        )

    log.info("Source validation OK for %s: %s rows (median %s)", ds, row_count, median_14d)
    context["ti"].xcom_push(key="row_count", value=int(row_count))


def assert_dynamic_table_freshness(**context):
    """Verify each dynamic table actually met its declared TARGET_LAG.

    We do NOT force a refresh (that would contradict TARGET_LAG and double the
    warehouse cost). We verify Snowflake honoured the contract, and alert if not.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    sf = SnowflakeHook(snowflake_conn_id="snowflake_prod")
    breaches = []

    for table, max_lag_min in DYNAMIC_TABLE_LAG_MINUTES.items():
        row = sf.get_first(
            """
            SELECT DATEDIFF('minute', MAX(refresh_end_time), CURRENT_TIMESTAMP())
            FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME => %s))
            WHERE STATE = 'SUCCEEDED'
            """,
            parameters=(table,),
        )
        actual_lag = row[0] if row and row[0] is not None else None

        if actual_lag is None:
            breaches.append(f"{table}: no successful refresh on record")
        elif actual_lag > max_lag_min * 1.5:  # 50% grace before paging
            breaches.append(
                f"{table}: {actual_lag} min stale vs TARGET_LAG {max_lag_min} min"
            )
        else:
            log.info("%s freshness OK: %s min (target %s)", table, actual_lag, max_lag_min)

    if breaches:
        raise AirflowException(
            "Dynamic table freshness contract breached:\n  - " + "\n  - ".join(breaches)
            + "\nRunbook: docs/10_runbooks.md#dynamic-table-lag"
        )


def generate_compliance_report(**context):
    """GDPR / CCPA daily compliance report.

    This task is the evidence an auditor asks for. It must run even when the
    analytics pipeline fails, hence its independent branch and
    trigger_rule=ALL_DONE.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg = PostgresHook(postgres_conn_id="stripe_oltp")

    # 1. GDPR erasure SLA — art.12(3): one month to respond.
    pending_sla_breach = pg.get_first(
        """
        SELECT COUNT(*) FROM gdpr_erasure_requests
        WHERE status IN ('pending', 'processing')
          AND sla_due_at < now()
        """
    )[0]

    # 2. Requests approaching the deadline (early warning, not yet a breach)
    at_risk = pg.get_first(
        """
        SELECT COUNT(*) FROM gdpr_erasure_requests
        WHERE status IN ('pending', 'processing')
          AND sla_due_at BETWEEN now() AND now() + INTERVAL '7 days'
        """
    )[0]

    # 3. Erasures actually executed in the last 24h.
    #    FIXED: was `operation = 'U'` + JSONB needle while the procedure wrote
    #    'D' → always returned 0. Now both sides agree, and `reason` is an
    #    indexed column rather than a silently-failing JSONB path.
    anonymised_24h = pg.get_first(
        """
        SELECT COUNT(*) FROM audit_log
        WHERE table_name = 'customers'
          AND operation  = 'U'
          AND reason     = 'GDPR_erasure_request'
          AND changed_at >= now() - INTERVAL '24 hours'
        """
    )[0]

    # 4. Erasures stuck mid-propagation (Postgres done, Mongo/Snowflake not).
    #    This is the failure mode that makes an org *believe* it is compliant.
    stuck_propagation = pg.get_first(
        """
        SELECT COUNT(*) FROM gdpr_erasure_requests
        WHERE status = 'processing'
          AND postgres_done = true
          AND (mongodb_done = false OR snowflake_done = false)
          AND requested_at < now() - INTERVAL '24 hours'
        """
    )[0]

    # 5. CCPA opt-outs processed today
    ccpa_optouts_today = pg.get_first(
        """
        SELECT COUNT(*) FROM ccpa_requests
        WHERE request_type = 'opt_out'
          AND status = 'completed'
          AND completed_date >= CURRENT_DATE
        """
    )[0]

    report = {
        "report_date": context["ds"],
        "gdpr_sla_breached": int(pending_sla_breach),
        "gdpr_at_risk_7d": int(at_risk),
        "gdpr_anonymised_last_24h": int(anonymised_24h),
        "gdpr_stuck_propagation": int(stuck_propagation),
        "ccpa_optouts_today": int(ccpa_optouts_today),
    }
    log.info("GDPR/CCPA compliance report: %s", json.dumps(report, indent=2))
    context["ti"].xcom_push(key="compliance_report", value=report)

    problems = []
    if pending_sla_breach > 0:
        problems.append(f"{pending_sla_breach} erasure request(s) past the 30-day deadline")
    if stuck_propagation > 0:
        problems.append(
            f"{stuck_propagation} erasure(s) applied in PostgreSQL but NOT propagated "
            "to MongoDB/Snowflake for >24h — data subject is not actually erased"
        )
    if problems:
        raise AirflowException("GDPR BREACH:\n  - " + "\n  - ".join(problems))

    return report


with DAG(
    dag_id="stripe_daily_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stripe", "olap", "compliance", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Analytics branch ──────────────────────────────────────
    validate = PythonOperator(
        task_id="validate_sources",
        python_callable=validate_sources,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select tag:daily --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --select tag:daily --target prod",
    )

    assert_freshness = PythonOperator(
        task_id="assert_dynamic_table_freshness",
        python_callable=assert_dynamic_table_freshness,
    )

    # ── Compliance branch (independent of analytics) ──────────
    compliance = PythonOperator(
        task_id="generate_compliance_report",
        python_callable=generate_compliance_report,
        # Legal deadlines do not care whether dbt succeeded.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> validate >> dbt_run >> dbt_test >> assert_freshness
    start >> compliance
