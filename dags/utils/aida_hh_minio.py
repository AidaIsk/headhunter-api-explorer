import time
import requests
import pandas as pd
from typing import List, Dict, Any
from botocore.exceptions import NoCredentialsError  

# Настройки API и фильтров

BASE_URL = "https://api.hh.ru/vacancies"  
PAGE_SIZE = 100                               # сколько записей на странице
RATE_LIMIT_SLEEP = 2                          # пауза между запросами (секунды)
minio_bucket = os.getenv("MINIO_BUCKET")
folder = "bronze/"

def get_s3_client():
    import os
    import boto3

    return boto3.client('s3',
                        aws_access_key_id = os.getenv("MINIO_ACCESS_KEY"),
                        aws_secret_access_key = os.getenv("MINIO_SECRET_KEY"),
                        endpoint_url = f'http://{os.getenv("MINIO_ENDPOINT")}', 
                        region_name="us-east-1",
                    )

BASE_PARAMS = {
    "area": 40,                                 # 40 = Казахстан
    "text": '"data engineer" OR "инженер данных" OR "etl" OR "dwh"' 
            'OR "аналитик данных" OR "data analyst"'
            'OR "bi" OR "power bi" OR "sql"'
            'OR "ml" OR "machine learning" OR "ml engineer"'               # вакансии, где встречаются слова
}

# Заголовки HTTP-запроса (HH требует указать User-Agent)
HEADERS =  {"User-Agent": "hh-remote-track/0.1 (aida.aitymova@gmail.com)"}

# Запрос одной страницы API

def fetch_page(page: int) -> Dict[str, Any]:
    """
    Отправляет запрос на HH API и возвращает JSON.
    Если запрос упал — возвращает пустой словарь, чтобы не ломать программу.
    """
    params = {
        **BASE_PARAMS,                      # фильтры вакансий
        "page": page,                       # номер страницы
        "per_page": PAGE_SIZE,              # сколько записей на странице
    }
    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()         # проверка успешности ответа
    except requests.exceptions.RequestException as e:
        return {}    
    
    return response.json()

# Извлечение списка вакансий из ответа API

def extract_items_from_response(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    HH API всегда возвращает вакансии под ключом "items".
    Если его нет — возвращаем пустой список.
    """

    return response_json.get("items", [])

# Запрос нескольких страниц (пагинация)

def fetch_all_items(max_pages: int = 19) -> List[Dict[str, Any]]:
    """
    Обходит страницы API по очереди: page=0, 1, 2, ...
    Останавливается, если:
     - получили пустую страницу
     - достигли max_pages
     - API вернуло меньше PAGE_SIZE записей (признак последней страницы)
    """
    all_items: List[Dict[str, Any]] = []
    page = 0

    # основной цикл пагинации
    while page <= max_pages:

        response_json = fetch_page(page)
        items = extract_items_from_response(response_json)

        # если данных нет — дальше страниц нет
        if not items:
            break

        all_items.extend(items)

        # если меньше, чем 100 — значит последняя страница
        if len(items) < PAGE_SIZE:
            break

        page += 1
        time.sleep(RATE_LIMIT_SLEEP)

    return all_items

# Преобразование списка JSON → DataFrame


def pipeline_hh_to_bronze_json(ds: str, **context):

    """
    Главная функция для Airflow: скачивает вакансии,
    преобразует и сохраняет в CSV.
    """

    items = fetch_all_items()

    try:  
        s3_client.put_object(Bucket=minio_bucket, Key = folder, Body=b"")  
        print(f"Folder '{folder}' created in bucket '{minio_bucket}'")  
    except NoCredentialsError:  
        print("Invalid AWS credentials provided")

        
    return items

