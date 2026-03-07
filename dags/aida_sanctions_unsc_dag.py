from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.sanctions_utils import ingest_unsc

default_args = {
    "owner": "aida",
    "start_date": datetime(2024,1,1),
    "retries": 1
}

with DAG(
    dag_id="ingest_unsc_sanctions",
    schedule_interval="0 21 * * 0",
    catchup=False,
    default_args=default_args
) as dag:

    ingest_task = PythonOperator(
        task_id="download_unsc_list",
        python_callable=ingest_unsc
    )