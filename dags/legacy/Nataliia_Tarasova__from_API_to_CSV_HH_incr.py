from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

import pandas as pd
from datetime import datetime, time
import pendulum

import utils.hh_loader_csv as hh_csv

doc_md = doc_md = """
# DAG «from_API_to_CSV_HH_incr»

### Цель
Инкрементальная ежедневная загрузка вакансий с портала [hh.ru](https://hh.ru/) по списку ключевых слов и регионам.  
Результаты сохраняются в **CSV файл** с дозаписью (`append`).

### Описание
- Источник данных: API hh.ru  
- Период выгрузки: **текущий execution_date** (с полуночи до 23:59:59)  
- Для каждого региона и каждого ключевого слова выполняется поиск вакансий  
- Все найденные вакансии объединяются и сохраняются в CSV (режим `mode="a"`, то есть добавление строк)

### Особенности
- Используются собственные утилиты `utils.hh_loader_csv`:
    - `get_regions()` — список регионов  
    - `KEYWORDS` — список ключевых слов для поиска  
    - `fetch_vacancies()` — запрос вакансий по региону и ключу  
    - `save_to_csv()` — сохранение результатов  

- DAG запускается каждый день в **07:00 UTC**  
- При сбое выполняется до **2 повторных запусков** с задержкой **3 минуты**  

### Таски
- **load_to_csv_incr** — основной таск, выполняющий инкрементальную загрузку вакансий и дозапись в CSV
"""
def send_telegram_message(**context):
    try:
        telegram_token = Variable.get('TG_BOT_TOKEN')
        chat_id = Variable.get('TG_BOT_CHAT_ID')

        dag_id = context['dag'].dag_id
        ti = context["dag_run"].get_task_instance("load_to_csv_incr")

        state = ti.state if ti else "unknown"

        if state == "success":
            status = "✅ *SUCCESS*"
            severity = "info"
        else:
            status = "❌ *FAILED*"
            severity = "critical"

        message = f"""
🔥 *Airflow Alert* 🔥

*DAG:* `{dag_id}`
*Task:* `load_to_csv_incr`
*Status:* {status}

*Severity:* {severity.upper()}
*Run ID:* `{ti.run_id if ti else 'N/A'}`

🕒 {ti.end_date.strftime('%Y-%m-%d %H:%M:%S') if ti and ti.end_date else 'N/A'}
        """

        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            },
            timeout=10
        )

        if resp.status_code != 200:
            logging.error(f"Telegram error: {resp.text}")

    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

def incremental_load(**context):
    start_date = datetime.combine(datetime.strptime(context['ds'], "%Y-%m-%d").date(), time(0, 0, 0))
    end_date = datetime.combine(datetime.strptime(context['ds'], "%Y-%m-%d").date(), time(23, 59, 59))

    regions = hh_csv.get_regions()
    all_vacancies = []

# ищем вакансии в каждом регионе по всем ключевым словам
    for region in regions:
        for kw in hh_csv.KEYWORDS:
            all_vacancies.extend(hh_csv.fetch_vacancies(region["id"], kw, start_date, end_date))

    hh_csv.save_to_csv(all_vacancies, mode="a")

default_args = {
    "owner": 'Nataliia_Tarasova',
    "retries": 2,
    "start_date": pendulum.datetime(2025, 9, 14, tz="UTC"), 
    "retry_delay": pendulum.duration(minutes=3)
}

# ---------------- DAG ----------------
with DAG(
    dag_id="Nataliia_Tarasova__from_API_to_CSV_HH_incr",
    schedule_interval="0 7 * * *",
    tags=["hh"],
    catchup=False,
    default_args=default_args
) as dag:

    dag.doc_md = doc_md

    load_task = PythonOperator(
        task_id="load_to_csv_incr",
        python_callable=incremental_load
    )

    telegram_notify = PythonOperator(
        task_id="send_telegram_notification",
        python_callable=send_telegram_message,
        trigger_rule=TriggerRule.ALL_DONE  # важно!
    )

    load_task >> telegram_notify