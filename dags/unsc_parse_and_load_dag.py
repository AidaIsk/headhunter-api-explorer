from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.sanctions_utils import get_latest_unsc_xml
from utils.unsc_parser import parse_unsc_xml
from utils.sanctions_loader import load_unsc_to_staging


default_args = {
    "owner": "aida",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}


def parse_and_load():

    # 1 получить XML
    xml_string = get_latest_unsc_xml()

    # 2 парсинг
    parsed = parse_unsc_xml(xml_string)

    # 3 загрузка
    load_unsc_to_staging(parsed)


with DAG(
    dag_id="aida_unsc_parse_and_load",
    schedule_interval="0 22 * * 0",
    catchup=False,
    default_args=default_args,
    tags=["sanctions", "regtech"]
) as dag:

    parse_and_load_task = PythonOperator(
        task_id="parse_and_load_unsc",
        python_callable=parse_and_load
    )

    parse_and_load_task