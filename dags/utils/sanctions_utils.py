import hashlib
import os
import json
import requests
from datetime import datetime
from pathlib import Path
import boto3
import psycopg2

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

def load_unsc_to_bronze(ds, **context):

    bucket = os.getenv("MINIO_BUCKET")
    s3 = get_s3_client()

    key = f"bronze/sanctions/source=UNSC/list=CONSOLIDATED/dt={ds}/consolidated.xml"

    local_path = f"/tmp/unsc_{ds}.xml"

    s3.download_file(bucket, key, local_path)

    with open(local_path, "r") as f:
        xml_data = f.read()

    conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO bronze.unsc_raw
        (source, ingestion_date, raw_xml)
        VALUES (%s,%s,%s)
        """,
        ("UNSC", ds, xml_data)
    )

    conn.commit()