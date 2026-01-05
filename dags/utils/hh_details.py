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

    expected_rows = load_vacancy_ids(ds, load_type)
    expected_set = {row['vacancy_id'] for row in expected_rows}
    expected_count = len(expected_set)

    prefix = f"bronze/hh/vacancy_details/load_type={load_type}/dt={ds}/"
    minio_bucket = os.getenv("MINIO_BUCKET")

    s3_client = get_s3_client()

    resp = s3_client.list_objects_v2(
        Bucket=minio_bucket, 
        Prefix=prefix,
        )

    contents = resp.get("Contents", [])
    keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".jsonl")]

    print(f"expected_count={expected_count}")
    print(f"found_files={len(keys)}")
    print("sample keys:", keys[:3])

    loaded_set = set()

    for key in keys:
        response = s3_client.get_object(Bucket=minio_bucket, Key=key)
        for line in response['Body'].iter_lines():
            if not line:
                continue
            row = json.loads(line.decode("utf-8"))
            loaded_set.add(row['id'])

    loaded_count = len(loaded_set)
    missing_set = expected_set - loaded_set
    missing_count = len(missing_set)
    coverage_pct = 0.0 if expected_count == 0 else loaded_count / expected_count * 100

    print(f"loaded_count={loaded_count}")
    print(f"missing_count={missing_count}")
    print(f"coverage_pct={coverage_pct}")

    print("sample_missing_ids:", list(sorted(missing_set)[:5]))

    report = dict()
    report['ds'] = ds
    report['load_type'] = load_type
    report['expected_count'] = expected_count
    report['loaded_count'] = loaded_count
    report['missing_count'] = missing_count
    report['coverage_pct'] = coverage_pct
    report['sample_missing_ids'] = list(sorted(missing_set)[:5])
    report['found_files'] = len(keys)
    report["severity"] = 'OK' if missing_count == 0 else ('WARNING' if (missing_count <= 5 or coverage_pct >= 99) else 'CRITICAL')

    object_key = f'bronze/hh/reports/vacancy_details_coverage/load_type={load_type}/dt={ds}/report.json'

    local_path = f"/tmp/vacancy_details_coverage_{ds}_{load_type}.json"
    Path("/tmp").mkdir(parents=True, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    s3_client.upload_file(local_path, minio_bucket, object_key)

    s3_path = f"s3://{minio_bucket}/{object_key}"
    print(s3_path)
    return s3_path

def collect_vacancy_details(ds: str, load_type: str, batch_size: int = 200):

    expected_ids = load_vacancy_ids(ds, load_type)
    batches = split_into_batches(expected_ids, batch_size=batch_size)

    all_failed_ids = []

    for i, batch in enumerate(batches):
        batch_idx = i
        details_rows, failed_ids = fetch_vacancy_details_batch(batch, ds, load_type, batch_idx)
        save_details_batch_to_minio(details_rows, ds, load_type, batch_idx)
        all_failed_ids.append(failed_ids)

    print("total expected_ids: ", len(expected_ids))
    print("total batches: ", len(batches))
    print("total failed_ids: ", len(all_failed_ids))
    print("failed_ids: ", all_failed_ids[:3])

