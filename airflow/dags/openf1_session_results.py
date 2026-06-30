import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from openf1_de.fetch_bronze_session_results import main

DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/config")

PROJECT = os.environ["PROJECT"].strip()
DATASET = os.environ["DATASET"].strip()
BQ_RAW_DATASET = os.environ["BQ_RAW_DATASET"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_MEDAL_FETCH = os.environ["GCS_MEDAL_FETCH"]
GCS_PUSH_BUCKET = os.environ["GCS_PUSH_BUCKET"].strip()
YEAR = int(os.environ["YEAR"])
YEARS = [y.strip() for y in os.environ.get("LOAD_YEARS", str(YEAR)).split(",") if y.strip()]

DRIVERS_SOURCE = [f"{GCS_MEDAL_FETCH}/drivers/{y}.parquet" for y in YEARS]
SR_SOURCE = [f"{GCS_MEDAL_FETCH}/session_results/{y}.parquet" for y in YEARS]

DRIVERS_BQ_TABLE = f"{PROJECT}.{BQ_RAW_DATASET}.raw_drivers"
SR_BQ_TABLE = f"{PROJECT}.{BQ_RAW_DATASET}.raw_session_results"
MART_LOCATION = f"{PROJECT}.{DATASET}"

SR_RACE_EXPORT = f"gs://{GCS_PUSH_BUCKET}/session_results/race_{YEAR}.json"
SR_QUALI_EXPORT = f"gs://{GCS_PUSH_BUCKET}/session_results/quali_{YEAR}.json"
SR_FP_EXPORT = f"gs://{GCS_PUSH_BUCKET}/session_results/fp_{YEAR}.json"
DRIVERS_EXPORT = f"gs://{GCS_PUSH_BUCKET}/drivers/{YEAR}.json"
DRIVER_STANDINGS_EXPORT = f"gs://{GCS_PUSH_BUCKET}/driver_standings/{YEAR}.json"
CONSTRUCTOR_STANDINGS_EXPORT = f"gs://{GCS_PUSH_BUCKET}/constructor_standings/{YEAR}_verify.json"

with DAG(
    dag_id="02_openf1_fetch_every_sat_sun_mon",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["5"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_every_sat_sun_mon",
        python_callable=main,
    )

    load_raw_drivers = GCSToBigQueryOperator(
        task_id="load_raw_drivers_parquet",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=DRIVERS_SOURCE,
        destination_project_dataset_table=DRIVERS_BQ_TABLE,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    load_raw_srs = GCSToBigQueryOperator(
        task_id="load_raw_srs_parquet",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=SR_SOURCE,
        destination_project_dataset_table=SR_BQ_TABLE,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_session_results",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select stg_session_results_race stg_drivers stg_driver_standings "
            f"stg_session_results_qualifying stg_session_results_fp "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts_srs",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart_session_results_race mart_drivers "
            f"mart_session_results_quali mart_session_results_fp "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_standings_marts = BashOperator(
        task_id="dbt_run_standings_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart_driver_standings mart_constructor_standings "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    export_mart_srs_race = BigQueryToGCSOperator(
        task_id="export_mart_srs_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_session_results_race",
        destination_cloud_storage_uris=[SR_RACE_EXPORT],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_drivers = BigQueryToGCSOperator(
        task_id="export_mart_drivers_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_drivers",
        destination_cloud_storage_uris=[DRIVERS_EXPORT],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_driver_standings = BigQueryToGCSOperator(
        task_id="export_mart_driver_standings_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_driver_standings",
        destination_cloud_storage_uris=[DRIVER_STANDINGS_EXPORT],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_constructor_standings = BigQueryToGCSOperator(
        task_id="export_mart_constructor_standings_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_constructor_standings",
        destination_cloud_storage_uris=[CONSTRUCTOR_STANDINGS_EXPORT],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_srqs = BigQueryToGCSOperator(
        task_id="export_mart_srq_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_session_results_quali",
        destination_cloud_storage_uris=[SR_QUALI_EXPORT],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    export_mart_srfps = BigQueryToGCSOperator(
        task_id="export_mart_srfp_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{MART_LOCATION}.mart_session_results_fp",
        destination_cloud_storage_uris=[SR_FP_EXPORT],
        export_format="NEWLINE_DELIMITED_JSON",
    )

    (
        ingest
        >> [load_raw_drivers, load_raw_srs]
        >> dbt_run_staging
        >> dbt_run_marts
        >> dbt_run_standings_marts
        >> [
            export_mart_srs_race,
            export_mart_drivers,
            export_mart_driver_standings,
            export_mart_constructor_standings,
            export_mart_srqs,
            export_mart_srfps,
        ]
    )
