import json
from pathlib import Path
import boto3
import os
import requests


BASE_URL = "https://api.hh.ru/vacancies"  
HEADERS =  {"User-Agent": "hh-remote-track/0.1 (aida.aitymova@gmail.com)"}


def get_s3_client():

    return boto3.client('s3',
                        aws_access_key_id = os.getenv("MINIO_ACCESS_KEY"),
                        aws_secret_access_key = os.getenv("MINIO_SECRET_KEY"),
                        endpoint_url = f'http://{os.getenv("MINIO_ENDPOINT")}', 
                        region_name="us-east-1",
                    )

def load_vacancy_ids(ds: str, load_type: str) -> list[dict]:

    minio_bucket = os.getenv("MINIO_BUCKET")
    source_key = f'bronze/hh/vacancies_ids/load_type={load_type}/dt={ds}/part-000.jsonl'
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=minio_bucket, Key=source_key)

    ids = []

    for line in response['Body'].iter_lines():
        if not line:
            continue
        row = json.loads(line.decode("utf-8"))
        ids.append(row)

    print(f"loaded vacancy ids: {len(ids)}")
    return ids

def split_into_batches(rows: list[dict], batch_size: int = 200) -> list[list[dict]]:
    batches = []
    total = len(rows)

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        batches.append(batch)
    
    return batches

def fetch_vacancy_details_batch(batch: list[dict], ds: str, load_type: str, batch_idx: int):
    details_rows = []
    failed_ids = []
    for row in batch:
        vacancy_id = row["vacancy_id"]
        URL = f'{BASE_URL}/{vacancy_id}'
        try:
            response = requests.get(
                URL,
                headers=HEADERS,
                timeout=30,
            )
            response.raise_for_status()         
        except requests.exceptions.RequestException as e:
            failed_ids.append(vacancy_id)
            continue
        detail = response.json()
        detail['search_profile'] = row.get('search_profile')
        detail['expected_risk_category'] = row.get('expected_risk_category')
        detail['load_dt']=ds
        detail['load_type']=load_type
        detail['batch_idx']=batch_idx
        details_rows.append(detail)
    return details_rows, failed_ids

def save_details_batch_to_minio(details_rows: list[dict], ds: str, load_type: str, batch_idx: int) -> str:
    
    local_path = f"/tmp/vacancy_details_{ds}_batch_{batch_idx:03d}.jsonl"
    Path("/tmp").mkdir(parents=True, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        for item in details_rows:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client()
    BRONZE_BASE_PREFIX = f'bronze/hh/vacancy_details'
    object_key = f"{BRONZE_BASE_PREFIX}/load_type={load_type}/dt={ds}/batch={batch_idx:03d}/part-000.jsonl"

    s3_client.upload_file(local_path, minio_bucket, object_key)

    return f"s3://{minio_bucket}/{object_key}"

def build_details_coverage_report(ds: str, load_type: str):

    expected_ids = load_vacancy_ids(ds, load_type)

