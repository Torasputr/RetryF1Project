from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_MEDAL_FETCH = os.environ["GCS_MEDAL_FETCH"]
YEAR = os.environ["YEAR"]
MEETINGS_SOURCE = f"{GCS_MEDAL_FETCH}/meetings/{YEAR}.parquet"
SESSIONS_SOURCE = f"{GCS_MEDAL_FETCH}/sessions/{YEAR}.parquet"
DRIVERS_SOURCE = f"{GCS_MEDAL_FETCH}/drivers/{YEAR}.parquet"
SR_SOURCE = f"{GCS_MEDAL_FETCH}/session_results/{YEAR}.parquet"
PROJECT = os.environ["PROJECT"]
BQ_RAW_DATASET = os.environ["BQ_RAW_DATASET"]
MEETINGS_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_meetings"
SESSIONS_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_sessions"
DRIVERS_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_drivers"
SR_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_session_results"

with DAG(
    dag_id="openf1_bq_load_meetings",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags={"2", "6"}
) as dag:
    load_raw_meetings = GCSToBigQueryOperator(
        task_id="load_raw_meetings_parquet",
        gcp_conn_id="google_cloud_default",  # Airflow Connection with your GCP creds
        bucket=GCS_BUCKET,
        source_objects=[MEETINGS_SOURCE],  # object path inside bucket
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
        source_objects=[SESSIONS_SOURCE],
        destination_project_dataset_table=SESSIONS_DESTINATION,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True
    )

    load_raw_drivers = GCSToBigQueryOperator(
        task_id="load_raw_drivers_parquet",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=[DRIVERS_SOURCE],
        destination_project_dataset_table=DRIVERS_DESTINATION,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True
    )

    load_raw_srs = GCSToBigQueryOperator(
        task_id="load_raw_srs_parquet",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=[SR_SOURCE],
        destination_project_dataset_table=SR_DESTINATION,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True
    )

    [load_raw_meetings, load_raw_sessions, load_raw_drivers, load_raw_srs]