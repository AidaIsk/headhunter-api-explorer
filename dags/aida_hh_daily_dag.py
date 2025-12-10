from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.aida_hh_api import pipeline_hh_to_csv

default_args = {
    "owner" : "aida",
    "retries": 1,
}

with DAG(
    dag_id="aida_hh_daily_csv",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["aida", "hh", "daily"],
) as dag:
    
    collect_daily_csv = PythonOperator(
        task_id="collect_hh_daily_csv",
        python_callable=pipeline_hh_to_csv,
        op_kwargs={
            "output_path": "/opt/airflow/dags/data/aida_hh_{{ ds }}.csv",
        },
    )

    collect_daily_csv