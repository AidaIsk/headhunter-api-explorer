from datetime import datetime
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from utils.hh_details import build_details_coverage_report
from utils.hh_details import collect_vacancy_details

default_args = {
    "owner" : "aida",
    "retries": 1,
}

with DAG(
    dag_id="aida_hh_details_daily",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["aida", "hh_details", "daily"],
) as dag:
    
    collect_vacancy_details_task = PythonOperator(
        task_id="collect_vacancy_details",
        python_callable=collect_vacancy_details,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily",
            "batch_size": 200,
        },
    )
    build_details_coverage_report_task = PythonOperator(
        task_id="build_details_coverage_report",
        python_callable=build_details_coverage_report,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily"
        },
    )
    

    collect_vacancy_details_task >> build_details_coverage_report_task
