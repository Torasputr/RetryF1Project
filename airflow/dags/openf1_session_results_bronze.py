from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from openf1_de.fetch_bronze_session_results import main

with DAG(
    dag_id="openf1_fetch_every_sat_sun_mon",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["5"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_every_sat_sun_mon",
        python_callable=main,
    )
    