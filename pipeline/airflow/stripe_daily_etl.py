from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True
}

with DAG(
    dag_id="stripe_daily_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stripe", "olap", "production"]
) as dag:

    validate_sources = SnowflakeOperator(
        task_id="validate_sources",
        sql="SELECT COUNT(*) FROM staging.raw_transactions WHERE load_date = CURRENT_DATE",
        snowflake_conn_id="snowflake_prod"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select tag:daily --target prod"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --select tag:daily --target prod"
    )

    refresh_views = SnowflakeOperator(
        task_id="refresh_views",
        sql="ALTER MATERIALIZED VIEW mv_daily_revenue REFRESH"
    )

    compliance_report = PythonOperator(
        task_id="generate_compliance_report",
        python_callable=generate_gdpr_report  # function to be defined
    )

    validate_sources >> dbt_run >> dbt_test >> refresh_views >> compliance_report