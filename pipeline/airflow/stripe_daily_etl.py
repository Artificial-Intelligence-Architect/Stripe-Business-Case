from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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


def generate_gdpr_report(**context):
    """
    GDPR / CCPA daily compliance report.

    Checks performed:
      1. Count of pending erasure requests older than 30 days (SLA breach risk)
      2. Count of CCPA opt-out requests processed today
      3. Count of customers anonymised in the last 24 h
      4. Raises an AirflowException if any SLA threshold is breached,
         which triggers the DAG retry / alerting chain.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.exceptions import AirflowException

    pg = PostgresHook(postgres_conn_id="stripe_oltp")

    # ------------------------------------------------------------------
    # 1. GDPR erasure SLA — requests pending for more than 30 days
    # ------------------------------------------------------------------
    pending_sla_breach = pg.get_first("""
        SELECT COUNT(*)
        FROM ccpa_requests
        WHERE status = 'pending'
          AND request_date < NOW() - INTERVAL '30 days'
          AND request_type = 'deletion'
    """)[0]

    # ------------------------------------------------------------------
    # 2. CCPA opt-out requests processed today
    # ------------------------------------------------------------------
    ccpa_optouts_today = pg.get_first("""
        SELECT COUNT(*)
        FROM ccpa_requests
        WHERE request_type  = 'opt_out'
          AND status        = 'completed'
          AND completed_date >= CURRENT_DATE
    """)[0]

    # ------------------------------------------------------------------
    # 3. Customers anonymised in the last 24 h (GDPR erasures executed)
    # ------------------------------------------------------------------
    anonymised_24h = pg.get_first("""
        SELECT COUNT(*)
        FROM audit_log
        WHERE table_name  = 'customers'
          AND operation   = 'U'
          AND new_values->>'reason' = 'GDPR_erasure_request'
          AND changed_at >= NOW() - INTERVAL '24 hours'
    """)[0]

    report = {
        "report_date":             context["ds"],
        "pending_gdpr_sla_breach": int(pending_sla_breach),
        "ccpa_optouts_today":      int(ccpa_optouts_today),
        "anonymised_last_24h":     int(anonymised_24h),
    }

    log.info("GDPR/CCPA compliance report: %s", json.dumps(report, indent=2))

    # Push to XCom for downstream tasks or monitoring dashboards
    context["ti"].xcom_push(key="compliance_report", value=report)

    # SLA enforcement: fail the task if erasure requests are overdue
    if pending_sla_breach > 0:
        raise AirflowException(
            f"GDPR SLA BREACH: {pending_sla_breach} deletion request(s) "
            f"pending for more than 30 days. Immediate action required."
        )


with DAG(
    dag_id="stripe_daily_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stripe", "olap", "production"],
    doc_md="""
    ## Stripe Daily ETL

    Orchestrates the full daily pipeline:
    `validate_sources → dbt_run → dbt_test → refresh_views → compliance_report`

    **SLA**: previous day's data available in Snowflake before 06:00 UTC.
    **On failure**: Slack + PagerDuty alert via Airflow notifier.
    """,
) as dag:

    validate_sources = SnowflakeOperator(
        task_id="validate_sources",
        sql="SELECT COUNT(*) FROM staging.raw_transactions WHERE load_date = CURRENT_DATE",
        snowflake_conn_id="snowflake_prod",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select tag:daily --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --select tag:daily --target prod",
    )

    refresh_views = SnowflakeOperator(
        task_id="refresh_views",
        sql="ALTER DYNAMIC TABLE mv_daily_revenue REFRESH;
ALTER DYNAMIC TABLE mv_customer_monthly REFRESH;",
        snowflake_conn_id="snowflake_prod",
    )

    compliance_report = PythonOperator(
        task_id="generate_compliance_report",
        python_callable=generate_gdpr_report,
    )

    validate_sources >> dbt_run >> dbt_test >> refresh_views >> compliance_report