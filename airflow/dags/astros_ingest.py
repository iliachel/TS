from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.ingest import fetch_and_insert


with DAG(
    dag_id="astros_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=4,
    max_active_tasks=4,
    tags={"clickhouse", "ingest"},
) as dag:
    PythonOperator(
        task_id="fetch_and_insert",
        python_callable=fetch_and_insert,
    )
