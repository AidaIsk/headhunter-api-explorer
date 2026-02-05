from pathlib import Path
import boto3
import os
import json

from airflow.exceptions import AirflowSkipException
from botocore.exceptions import ClientError

import logging


def get_s3_client():

    return boto3.client('s3',
                        aws_access_key_id = os.getenv("MINIO_ACCESS_KEY"),
                        aws_secret_access_key = os.getenv("MINIO_SECRET_KEY"),
                        endpoint_url = f'http://{os.getenv("MINIO_ENDPOINT")}', 
                        region_name="us-east-1",
                    )
    
def build_vacancies_ids_manifest(ds, load_type, **context):
        
    local_path = f"/tmp/vacancies_ids_{ds}.jsonl"
    Path("/tmp").mkdir(parents=True, exist_ok=True)
    minio_bucket = os.getenv("MINIO_BUCKET")

    source_key = f'bronze/hh/vacancies_list/load_type={load_type}/dt={ds}/part-000.jsonl'
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=minio_bucket, Key=source_key)

    seen_ids = set()

    with open(local_path, "w", encoding="utf-8") as f:
        for line in response['Body'].iter_lines():
            if not line:
                continue
            obj = json.loads(line.decode('utf-8'))
            vacancy_id = obj.get('id')
            if not vacancy_id or vacancy_id in seen_ids:
                continue
        
            seen_ids.add(vacancy_id)

            manifest_row = {
                "vacancy_id": vacancy_id,
                "search_profile": obj.get("search_profile"),
                "expected_risk_category": obj.get("expected_risk_category"),
                "load_dt": obj.get("load_dt", ds),
                "load_type": obj.get("load_type", load_type),
            }

            f.write(json.dumps(manifest_row, ensure_ascii=False) + '\n')

    print(f"manifest ids: {len(seen_ids)}")

    BRONZE_BASE_PREFIX = f'bronze/hh/vacancies_ids'
    object_key = f"{BRONZE_BASE_PREFIX}/load_type={load_type}/dt={ds}/part-000.jsonl"

    s3_client.upload_file(local_path, minio_bucket, object_key)

    return f"s3://{minio_bucket}/{object_key}"

def guard_has_ids(ds, load_type, **context):

    minio_bucket = os.getenv("MINIO_BUCKET")

    manifest_key = (
        f"bronze/hh/vacancies_ids/"
        f"load_type={load_type}/dt={ds}/part-000.jsonl"
    )

    s3_client = get_s3_client()

    try:
        s3_client.head_object(
            Bucket=minio_bucket,
            Key=manifest_key
        )
        logging.info(
            f"Manifest found: s3://{minio_bucket}/{manifest_key}"
        )

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code == "404":
            logging.warning(
                f"Manifest NOT found for ds={ds}, load_type={load_type}. "
                "Skipping enrichment DAG."
            )
            raise AirflowSkipException(
                f"No vacancies_ids manifest for ds={ds}"
            )

        logging.error(
            f"Error while checking manifest existence: {e}"
        )
        raise