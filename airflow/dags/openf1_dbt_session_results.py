import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/openf1_raw"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/config")

PROJECT = os.environ.get("PROJECT").strip()
DATASET = os.environ.get("DATASET").strip()
MART_LOCATION = f"{PROJECT}.{DATASET}"
GCS_PUSH_BUCKET = os.environ.get("GCS_PUSH_BUCKET").strip()
YEAR = int(os.environ.get("YEAR"))

SR_RACE_EXPORT = f"gs://{GCS_PUSH_BUCKET}/session_results/race_{YEAR}.json"
SR_QUALI_EXPORT = f"gs://{GCS_PUSH_BUCKET}/session_results/quali_{YEAR}.json"
SR_FP_EXPORT = f"gs://{GCS_PUSH_BUCKET}/session_results/fp_{YEAR}.json"
DRIVERS_EXPORT = f"gs://{GCS_PUSH_BUCKET}/drivers/{YEAR}.json"
DRIVER_STANDINGS_EXPORT = f"gs://{GCS_PUSH_BUCKET}/driver_standings/{YEAR}.json"
CONSTRUCTOR_STANDINGS_EXPORT = f"gs://{GCS_PUSH_BUCKET}/constructor_standings/{YEAR}_verify.json"

with DAG(
    dag_id="openf1_dbt_session_results",
    description="Run dbt marts for session results (assumes raw tables already loaded)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["7"],
) as dag:
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
        dbt_run_marts
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
