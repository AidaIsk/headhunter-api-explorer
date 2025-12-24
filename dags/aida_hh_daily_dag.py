from datetime import datetime
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from utils.aida_hh_minio import pipeline_hh_to_bronze_json

default_args = {
    "owner" : "aida",
    "retries": 1,
}

def send_telegram_message(**context):
    try:
        telegram_token = Variable.get('TG_BOT_TOKEN')
        chat_id = Variable.get('TG_BOT_CHAT_ID')

        dag_id = context['dag'].dag_id
        ti_load = context["dag_run"].get_task_instance("collect_hh_bronze_json")
        
        load_state = ti_load.state

        if load_state == "success":
            status = "✅ *SUCCESS*"
            severity = "info"
        else:
            status = "❌ *FAILED*"
            severity = "critical"

        message = f"""
🔥 *Airflow Alert* 🔥

*DAG:* `{dag_id}`
*Task:* `collect_hh_bronze_json`
*Status:* {status}

*SEVERITY:* {severity.upper()}
*Run ID:* {ti_load.run_id}

🕒 {ti_load.end_date.strftime('%Y-%m-%d %H:%M:%S') if ti_load.end_date else 'N/A'}
        """

        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        })

        if resp.status_code != 200:
            logging.error(f"Ошибка отправки Telegram: {resp.text}")
        else:
            logging.info(f"Telegram уведомление отправлено: {message}")

    except Exception as e:
        logging.error(f"Не удалось отправить сообщение в Telegram: {e}")



with DAG(
    dag_id="aida_hh_daily_json",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["aida", "hh", "daily"],
) as dag:
    
    collect_bronze_json = PythonOperator(
        task_id="collect_hh_bronze_json",
        python_callable=pipeline_hh_to_bronze_json,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily"
        },
    )
    telegram_notify_task = PythonOperator(
        task_id='send_telegram_notification',
        python_callable=send_telegram_message,
        # запускается, есди даже load_task упал
        trigger_rule=TriggerRule.ALL_DONE
    )

    collect_bronze_json >> telegram_notify_task