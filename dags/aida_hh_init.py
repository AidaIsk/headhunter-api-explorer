from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.aida_hh_minio import pipeline_hh_to_bronze_json

default_args = {
    "owner" : "aida",
    "retries": 1,
}

with DAG(
    dag_id="aida_hh_init_bronze_json",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aida", "hh", "init"],
) as dag:
    
    collect_init_bronze_json = PythonOperator(
        task_id="collect_init_bronze_json",
        python_callable=pipeline_hh_to_bronze_json,
        op_kwargs={
            "ds":"{{ds}}}",
            "prefix":"hh_init",
        },
    )

    collect_init_bronze_json