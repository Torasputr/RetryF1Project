from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_MEDAL_FETCH = os.environ["GCS_MEDAL_FETCH"]
YEAR = os.environ["YEAR"]
SOURCE = f"{GCS_MEDAL_FETCH}/meetings/{YEAR}.parquet"
PROJECT = os.environ["PROJECT"]
BQ_RAW_DATASET = os.environ["BQ_RAW_DATASET"]
DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_meetings"

with DAG(
    dag_id="openf1_bq_load_meetings",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    load_raw_meetings = GCSToBigQueryOperator(
        task_id="load_raw_meetings_parquet",
        gcp_conn_id="google_cloud_default",  # Airflow Connection with your GCP creds
        bucket=GCS_BUCKET,
        source_objects=[SOURCE],  # object path inside bucket
        destination_project_dataset_table=DESTINATION,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )