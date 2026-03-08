import hashlib
import os
import json
import requests
from datetime import datetime
from pathlib import Path
import boto3
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Официальный URL консолидированного списка ООН
UNSC_XML_URL = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        endpoint_url=f'http://{os.getenv("MINIO_ENDPOINT")}',
        region_name="us-east-1",
    )

def sha256_file(path: str) -> str:
    """Вычисляет SHA256 хеш файла для аудита данных."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def ingest_unsc(ds, **context):
    """Скачивает список ООН, считает хеш и загружает в MinIO Bronze."""
    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client() 
    
    # 1. Скачивание во временный файл
    local_path = f"/tmp/unsc_{ds}.xml"
    response = requests.get(UNSC_XML_URL, timeout=60)
    response.raise_for_status()
    
    with open(local_path, "wb") as f:
        f.write(response.content)
    
    # 2. Расчет хеша и метаданных
    file_hash = sha256_file(local_path)
    file_size = os.path.getsize(local_path)
    
    # 3. Формирование путей в MinIO
    base_prefix = f"bronze/sanctions/source=UNSC/list=CONSOLIDATED/dt={ds}"
    object_key = f"{base_prefix}/consolidated.xml"
    meta_key = f"{base_prefix}/metadata.json"
    
    # 4. Загрузка файла
    s3_client.upload_file(local_path, minio_bucket, object_key)
    
    # 5. Загрузка метаданных (контракт для аудита)
    metadata = {
        "source_url": UNSC_XML_URL,
        "downloaded_at_utc": datetime.utcnow().isoformat(),
        "sha256": file_hash,
        "content_length": file_size
    }
    
    local_meta_path = local_path + ".json"
    with open(local_meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
        
    s3_client.upload_file(local_meta_path, minio_bucket, meta_key)
    
    print(f"✅ Успешно загружен список UNSC за {ds}. Hash: {file_hash}")

def load_unsc_metadata(bucket, prefix, **context):

    s3_client = get_s3_client()

    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    paginator = s3_client.get_paginator("list_objects_v2")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):

                for obj in page.get("Contents", []):

                    key = obj["Key"]

                    if not key.endswith("metadata.json"):
                        continue

                    file_obj = s3_client.get_object(
                        Bucket=bucket,
                        Key=key
                    )

                    metadata = json.loads(
                        file_obj["Body"].read().decode("utf-8")
                    )

                    load_dt = datetime.utcnow()

                    cur.execute(
                        """
                        INSERT INTO bronze.sanctions_raw (
                            source,
                            load_dt,
                            object_key,
                            payload,
                            metadata,
                            sha256
                        )
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (sha256) DO NOTHING;
                        """,
                        (
                            "UNSC",
                            load_dt,
                            key,
                            None,
                            json.dumps(metadata),
                            metadata["sha256"]
                        )
                    )

        conn.commit()

