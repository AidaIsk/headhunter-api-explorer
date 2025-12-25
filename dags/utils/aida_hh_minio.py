import time
import requests
from typing import List, Dict, Any
import os
from botocore.exceptions import NoCredentialsError  
import json
from pathlib import Path
import boto3

from utils.search_profiles import load_enabled_search_profiles


# Настройки API и фильтров

BASE_URL = "https://api.hh.ru/vacancies"  
PAGE_SIZE = 100                               # сколько записей на странице
RATE_LIMIT_SLEEP = 2                          # пауза между запросами (секунды)


def get_s3_client():

    return boto3.client('s3',
                        aws_access_key_id = os.getenv("MINIO_ACCESS_KEY"),
                        aws_secret_access_key = os.getenv("MINIO_SECRET_KEY"),
                        endpoint_url = f'http://{os.getenv("MINIO_ENDPOINT")}', 
                        region_name="us-east-1",
                    )

BASE_PARAMS = {
    "area": 40,                                 # Казахстан — пилотный регион
}

# Заголовки HTTP-запроса (HH требует указать User-Agent)
HEADERS =  {"User-Agent": "hh-remote-track/0.1 (aida.aitymova@gmail.com)"}

# Запрос одной страницы API

def fetch_page(page: int, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Отправляет запрос на HH API и возвращает JSON.
    Если запрос упал — возвращает пустой словарь, чтобы не ломать программу.
    """
    params = {
        **params,                      # фильтры вакансий
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

def fetch_all_items(params: Dict[str, Any], max_pages: int = 19) -> List[Dict[str, Any]]:
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

        response_json = fetch_page(page, params)
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


def pipeline_hh_to_bronze_json(ds: str, load_type: str = "daily", **context):

    """
    Главная функция для Airflow: скачивает вакансии,
    преобразует и сохраняет в CSV.
    """
    enabled_profiles = load_enabled_search_profiles()
    items = []

    for profile in enabled_profiles:
        params = {**BASE_PARAMS, "text": profile["text"]}
        profile_items = fetch_all_items(params)

        print(profile["profile_id"], len(profile_items))

        for item in profile_items:
            item["search_profile"] = profile["profile_id"]
            item["expected_risk_category"] = profile["expected_risk_category"]
            item["load_dt"] = ds
            item["load_type"] = load_type

        items.extend(profile_items)


    local_path = f"/tmp/vacancies_{ds}.jsonl"
    Path("/tmp").mkdir(parents=True, exist_ok=True)
    
    with open(local_path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client()

    BRONZE_BASE_PREFIX = f'bronze/hh/vacancies_list'
    object_key = f"{BRONZE_BASE_PREFIX}/load_type={load_type}/dt={ds}/part-000.jsonl"
    print(f"ds = {ds}")
    print(f"items count = {len(items)}")
    print(f"MINIO_BUCKET = {minio_bucket}")
    print(f"object_key = {object_key}")
    s3_client.head_bucket(Bucket=minio_bucket)

    s3_client.upload_file(local_path, minio_bucket, object_key)  
    
    print(f"✅ Uploaded to s3://{minio_bucket}/{object_key}")
    print(f"local_path = {local_path}, bytes = {os.path.getsize(local_path)}")


        
    return f"s3://{minio_bucket}/{object_key}"

