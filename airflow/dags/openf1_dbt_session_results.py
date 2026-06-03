import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/config")

PROJECT = os.environ.get("PROJECT").strip()
DATASET = os.environ.get("DATASET").strip()
MART_LOCATION = f"{PROJECT}.{DATASET}"
GCS_PUSH_BUCKET = os.environ.get("GCS_PUSH_BUCKET").strip()
YEAR = int(os.environ.get("YEAR"))
SR_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/session_results/race_{YEAR}_*.json"
SRQ_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/session_results/quali_{YEAR}_*.json"
DS_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/driver_standings/{YEAR}_*.json"
CS_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/constructor_standings/{YEAR}_*.json"


with DAG(
    dag_id="openf1_dbt_session_results",
    description="Run dbt staging models for session results (assumes raw tables already loaded)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=['7']
) as dag:
    dbt_run_staging = BashOperator(
        task_id="dbt_run_session_results",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select stg_session_results_race stg_driver_standings stg_session_results_qualifying " 
            f"--profiles-dir {DBT_PROFILES_DIR} "
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts_srs",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart_session_results_race mart_drivers mart_constructor_standings mart_session_results_quali --profiles-dir {DBT_PROFILES_DIR} "
        ),
    )

    export_mart_srs_race = BigQueryToGCSOperator(
        task_id="export_mart_srs_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_session_results_race",
        destination_cloud_storage_uris=[SR_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_dss_race = BigQueryToGCSOperator(
        task_id="export_mart_dss_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_drivers",
        destination_cloud_storage_uris=[DS_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_css_race = BigQueryToGCSOperator(
        task_id="export_mart_css_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_constructor_standings",
        destination_cloud_storage_uris=[CS_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_srqs_race = BigQueryToGCSOperator(
        task_id="export_mart_srq_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_session_results_quali",
        destination_cloud_storage_uris=[SRQ_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    dbt_run_staging >> dbt_run_marts >> [export_mart_srs_race, export_mart_dss_race, export_mart_css_race, export_mart_srqs_race]