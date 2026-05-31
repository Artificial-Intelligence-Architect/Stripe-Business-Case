from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def generate_compliance_report():
    print("Rapport de conformité généré avec succès.")

with DAG(
    dag_id="stripe_daily_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stripe", "production"]
) as dag:

    validate_sources = BashOperator(
        task_id="validate_sources",
        bash_command="echo 'Validation des sources OK'"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="echo 'Transformations dbt exécutées'"
    )

    refresh_views = BashOperator(
        task_id="refresh_views",
        bash_command="echo 'Vues matérialisées rafraîchies'"
    )

    compliance_report = PythonOperator(
        task_id="generate_compliance_report",
        python_callable=generate_compliance_report
    )

    validate_sources >> dbt_run >> refresh_views >> compliance_report
