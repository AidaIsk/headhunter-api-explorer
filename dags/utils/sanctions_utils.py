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

def download_unsc(ds, **context):

    local_path = f"/tmp/unsc_{ds}.xml"

    response = requests.get(UNSC_XML_URL, timeout=60)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)

    return local_path

def upload_unsc_to_minio(ds, ti, **context):

    s3_client = get_s3_client()
    minio_bucket = os.getenv("MINIO_BUCKET")

    local_path = ti.xcom_pull(task_ids="download_unsc")

    file_hash = sha256_file(local_path)
    file_size = os.path.getsize(local_path)

    base_prefix = f"bronze/sanctions/source=UNSC/list=CONSOLIDATED/dt={ds}"

    xml_key = f"{base_prefix}/consolidated.xml"
    meta_key = f"{base_prefix}/metadata.json"

    s3_client.upload_file(local_path, minio_bucket, xml_key)

    metadata = {
        "source_url": UNSC_XML_URL,
        "downloaded_at_utc": datetime.utcnow().isoformat(),
        "sha256": file_hash,
        "content_length": file_size
    }

    local_meta = local_path + ".json"

    with open(local_meta, "w") as f:
        json.dump(metadata, f)

    s3_client.upload_file(local_meta, minio_bucket, meta_key)

    return {
        "bucket": minio_bucket,
        "xml_key": xml_key,
        "metadata": metadata
    }

def register_unsc_raw(ti, **context):

    data = ti.xcom_pull(task_ids="upload_unsc_to_minio")

    xml_key = data["xml_key"]
    metadata = data["metadata"]

    s3_client = get_s3_client()

    xml_obj = s3_client.get_object(
        Bucket=data["bucket"],
        Key=xml_key
    )

    payload = xml_obj["Body"].read().decode("utf-8")

    hook = PostgresHook(postgres_conn_id="postgres_bronze")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

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
                    datetime.utcnow(),
                    xml_key,
                    payload,
                    json.dumps(metadata),
                    metadata["sha256"]
                )
            )

        conn.commit()