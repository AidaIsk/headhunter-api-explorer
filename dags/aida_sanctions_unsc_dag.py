from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.sanctions_utils import download_unsc
from utils.sanctions_utils import upload_unsc_to_minio
from utils.sanctions_utils import register_unsc_raw

default_args = {
    "owner": "aida",
    "start_date": datetime(2024,1,1),
    "retries": 1
}

with DAG(
    dag_id="aida_ingest_unsc_sanctions",
    schedule_interval="0 21 * * 0",
    catchup=False,
    default_args=default_args
) as dag:

    download_task = PythonOperator(
        task_id="download_unsc",
        python_callable=download_unsc
    )

    upload_task = PythonOperator(
        task_id="upload_unsc_to_minio",
        python_callable=upload_unsc_to_minio
    )

    register_task = PythonOperator(
        task_id="register_unsc_raw",
        python_callable=register_unsc_raw,
        op_kwargs={
            "bucket": "sanctions"
        }
    )

    download_task >> upload_task >> register_task