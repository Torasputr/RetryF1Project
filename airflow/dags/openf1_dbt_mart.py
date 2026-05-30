import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR")

PROJECT = os.environ.get("PROJECT").strip()
DATASET = os.environ.get("DATASET").strip()
MART_LOCATION = f"{PROJECT}.{DATASET}"
GCS_PUSH_BUCKET = os.environ.get("GCS_PUSH_BUCKET").strip()
YEAR = int(os.environ.get("YEAR"))
SCHEDULE_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/schedule/{YEAR}_*.json"
DRIVERS_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/drivers/{YEAR}_*.json"

with DAG(
    dag_id="openf1_dbt_mart",
    description="Run dbt mart models and tests",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=['openf1', 'dbt', 'marts']
) as dag:
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    export_mart_schedule = BigQueryToGCSOperator(
        task_id="export_mart_schedule_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_schedule",
        destination_cloud_storage_uris=[SCHEDULE_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_drivers = BigQueryToGCSOperator(
        task_id="export_mart_drivers_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_drivers",
        destination_cloud_storage_uris=[DRIVERS_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    dbt_run_marts >> [export_mart_schedule, export_mart_drivers]