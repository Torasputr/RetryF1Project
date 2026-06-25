import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from openf1_de.fetch_bronze_meetings import main

DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/config")

PROJECT = os.environ["PROJECT"].strip()
DATASET = os.environ["DATASET"].strip()
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_MEDAL_FETCH = os.environ["GCS_MEDAL_FETCH"]
GCS_PUSH_BUCKET = os.environ["GCS_PUSH_BUCKET"].strip()
BQ_RAW_DATASET = os.environ["BQ_RAW_DATASET"]
YEAR = int(os.environ["YEAR"])
YEARS = [str(YEAR)]

MEETINGS_SOURCE = [f"{GCS_MEDAL_FETCH}/meetings/{y}.parquet" for y in YEARS]
SESSIONS_SOURCE = [f"{GCS_MEDAL_FETCH}/sessions/{y}.parquet" for y in YEARS]
MEETINGS_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_meetings"
SESSIONS_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_sessions"
MART_LOCATION = f"{PROJECT}.{DATASET}"
SCHEDULE_DESTINATION = f"gs://{GCS_PUSH_BUCKET}/schedule/new_{YEAR}.json"

with DAG(
    dag_id="01_openf1_fetch_once_per_season",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags={"1", "Fetch meetings and session", "Create mart_schedule"},
) as dag:
    ingest = PythonOperator(
        task_id="ingest_once_per_season",
        python_callable=main,
    )

    load_raw_meetings = GCSToBigQueryOperator(
        task_id="load_raw_meetings_parquet",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=MEETINGS_SOURCE,
        destination_project_dataset_table=MEETINGS_DESTINATION,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    load_raw_sessions = GCSToBigQueryOperator(
        task_id="load_raw_sessions_parquet",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=SESSIONS_SOURCE,
        destination_project_dataset_table=SESSIONS_DESTINATION,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging_for_schedule",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt clean --profiles-dir {DBT_PROFILES_DIR} && "
            f"dbt run --select stg_meetings stg_sessions "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_mart_schedule = BashOperator(
        task_id="dbt_run_mart_schedule",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart_schedule "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    export_mart_schedule = BigQueryToGCSOperator(
        task_id="export_mart_schedule_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_schedule",
        destination_cloud_storage_uris=[SCHEDULE_DESTINATION],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    (ingest >> [load_raw_meetings, load_raw_sessions] >> dbt_run_staging >> dbt_run_mart_schedule >> export_mart_schedule)
