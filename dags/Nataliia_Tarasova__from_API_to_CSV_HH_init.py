from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


import pandas as pd
from datetime import datetime, time
import pendulum

import utils.hh_loader_csv as hh_csv

doc_md = doc_md = """
# DAG «from_API_to_CSV_HH_init»

### Цель
Загрузка вакансий с портала [hh.ru](https://hh.ru/) по списку ключевых слов и регионам.  
Результаты сохраняются в **CSV файл** для последующего анализа.

### Описание
- Источник данных: API hh.ru
- Период выгрузки задаётся параметрами `start_date` и `end_date`
- Для каждого региона и ключевого слова выполняется поиск вакансий
- Результаты объединяются и сохраняются в единый CSV-файл

### Особенности
- Используются собственные утилиты: `utils.hh_loader_csv`
    - `get_regions()` — список регионов
    - `KEYWORDS` — список ключевых слов для поиска
    - `fetch_vacancies()` — запрос вакансий по региону и ключу
    - `save_to_csv()` — сохранение результатов
- Перезапуск возможен вручную (т.к. `schedule_interval=None`)
- При падении таска выполняется до **2 повторных запусков** с задержкой **3 минуты**

### Таски
- **load_to_csv_init** — основной таск, выполняющий загрузку вакансий и запись в CSV
"""


def initial_load(start_date, end_date):
    regions = hh_csv.get_regions()
    all_vacancies = []

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
 # ищем вакансии в каждом регионе по всем ключевым словам
    for region in regions:
        for kw in hh_csv.KEYWORDS:
            all_vacancies.extend(hh_csv.fetch_vacancies(region["id"], kw, start_date, end_date))

    hh_csv.save_to_csv(all_vacancies, mode="w")

default_args = {
    "owner": 'Nataliia_Tarasova',
    "retries": 2,
    "start_date": days_ago(1),
    "retry_delay": pendulum.duration(minutes=3)
}

# ---------------- DAG ----------------
with DAG(
    dag_id="Nataliia_Tarasova__from_API_to_CSV_HH_init",
    schedule_interval=None,
    tags=["hh"],
    catchup=False,
    default_args=default_args
) as dag:

    dag.doc_md = doc_md

    load_task = PythonOperator(
        task_id="load_to_csv_init",
        python_callable=initial_load,
        op_args=["2025-09-01", "2025-09-10"]
    )