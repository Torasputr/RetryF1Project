from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from openf1_de.fetch_bronze_meetings import main

with DAG(
    dag_id="openf1_meetings",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    ingest = PythonOperator(
        task_id="ingest_meetings",
        python_callable=main,
    )