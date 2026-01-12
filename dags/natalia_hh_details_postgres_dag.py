from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
import requests


import utils.natalia_hh_postgres as pg_utils

def send_telegram_notification(**context):
    try:
        telegram_token = Variable.get("TG_BOT_TOKEN")
        chat_id = Variable.get("TG_BOT_CHAT_ID")

        dag_id = context["dag"].dag_id
        ti = context["dag_run"].get_task_instance("load_files_to_postgres")

        state = ti.state if ti else "unknown"

        if state == "success":
            status = "✅ SUCCESS"
        else:
            status = "❌ FAILED"

        message = f"""
📦 *Airflow DAG notification*

*DAG:* `{dag_id}`
*Task:* `load_files_to_postgres`
*Status:* {status}
*Run ID:* `{ti.run_id}`

🕒 {ti.end_date.strftime('%Y-%m-%d %H:%M:%S') if ti.end_date else 'N/A'}
        """

        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
        )

        if resp.status_code != 200:
            logging.error(f"Telegram error: {resp.text}")

    except Exception as e:
        logging.error(f"Failed to send Telegram notification: {e}")


default_args = {
    "owner" : "nataliia",
    "retries": 1,
}

with DAG(
    dag_id='nataliia_hh_details_to_postgres',
    default_args=default_args,
    start_date=datetime(2026, 1, 4),
    schedule_interval='0 8 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:

    # Создаем при необходимости таблицу в постгрес
    init_postgres_tables_task = PythonOperator(
        task_id='init_postgres_tables',
        python_callable=pg_utils.init_postgres_tables,
        op_kwargs={
            "postgres_conn_id": "postgres_bronze",
            "ddl_path": "dags/utils/ddl_pg_bronze_details.sql"
        }
    )

    # Проверяем новые файлы в MinIO
    check_new_files_task = PythonOperator(
        task_id='check_new_files',
        python_callable=pg_utils.check_new_files_batch,
        op_kwargs={
        "bucket": "hh-raw",
        "prefix": "bronze/hh/vacancies_details/load_type=daily"
        }
    )

    # Загружаем новые файлы в Postgres
    load_to_postgres_task = PythonOperator(
        task_id='load_files_to_postgres',
        python_callable=pg_utils.load_to_postgres_batch,
        op_kwargs={
        "bucket": "hh-raw",
        "prefix": "bronze/hh/vacancies_details/load_type=daily"
        }
    )

    telegram_notify_task = PythonOperator(
    task_id="send_telegram_notification",
    python_callable=send_telegram_notification,
    trigger_rule=TriggerRule.ALL_DONE,  
)

    init_postgres_tables_task >> check_new_files_task >> load_to_postgres_task >> telegram_notify_task