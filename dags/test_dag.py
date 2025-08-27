from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG будет запускаться раз в день
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="Простой тестовый DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    task1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Airflow работает!'"
    )

    task2 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task1 >> task2
