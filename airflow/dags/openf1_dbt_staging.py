import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/config")

with DAG(
    dag_id="openf1_dbt_staging",
    description="Run dbt staging models and tests (assumes raw tables already loaded)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=['3', 'openf1', 'dbt', 'staging']
) as dag:
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select staging --exclude stg_session_results_race " 
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )