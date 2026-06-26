from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from openf1_de.fetch_bronze_session_results import main
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_MEDAL_FETCH = os.environ["GCS_MEDAL_FETCH"]
YEARS = [y.strip() for y in os.environ.get("LOAD_YEARS", "2025,2026").split(",")]
DRIVERS_SOURCE = [f"{GCS_MEDAL_FETCH}/drivers/{y}.parquet" for y in YEARS]
SR_SOURCE = [f"{GCS_MEDAL_FETCH}/session_results/{y}.parquet" for y in YEARS]
PROJECT = os.environ["PROJECT"]
BQ_RAW_DATASET = os.environ["BQ_RAW_DATASET"]
DRIVERS_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_drivers"
SR_DESTINATION = f"{PROJECT}.{BQ_RAW_DATASET}.raw_session_results"

DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/config")

with DAG(
    dag_id="02_openf1_fetch_every_sat_sun_mon",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["5"],
) as dag:
    # ingest = PythonOperator(
    #     task_id="ingest_every_sat_sun_mon",
    #     python_callable=main,
    # )
    # load_raw_drivers = GCSToBigQueryOperator(
    #     task_id="load_raw_drivers_parquet",
    #     gcp_conn_id="google_cloud_default",
    #     bucket=GCS_BUCKET,
    #     source_objects=DRIVERS_SOURCE,
    #     destination_project_dataset_table=DRIVERS_DESTINATION,
    #     source_format="PARQUET",
    #     write_disposition="WRITE_TRUNCATE",
    #     create_disposition="CREATE_IF_NEEDED",
    #     autodetect=True
    # )

    # load_raw_srs = GCSToBigQueryOperator(
    #     task_id="load_raw_srs_parquet",
    #     gcp_conn_id="google_cloud_default",
    #     bucket=GCS_BUCKET,
    #     source_objects=SR_SOURCE,
    #     destination_project_dataset_table=SR_DESTINATION,
    #     source_format="PARQUET",
    #     write_disposition="WRITE_TRUNCATE",
    #     create_disposition="CREATE_IF_NEEDED",
    #     autodetect=True
    # )
    
    dbt_run_staging = BashOperator(
        task_id="dbt_run_session_results",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select stg_session_results_race stg_drivers stg_driver_standings stg_session_results_qualifying stg_session_results_fp " 
            f"--profiles-dir {DBT_PROFILES_DIR} "
        ),
    )

    # (ingest >> [load_raw_drivers, load_raw_srs] >> dbt_run_staging)
    (dbt_run_staging)