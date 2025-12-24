import os, io, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "hh-raw")

def upload_df_to_minio():
    from minio import Minio  # импорт внутри функции
    df = pd.DataFrame({"id":[1,2,3],"name":["Alice","Bob","Charlie"]})
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    stream = io.BytesIO(csv_bytes)
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
    )   

    if not client.bucket_exists(bucket_name=MINIO_BUCKET):
        client.make_bucket(bucket_name=MINIO_BUCKET)

    key = f"test/{datetime.now():%Y/%m/%d}/pandas_{datetime.now():%H%M%S}.csv"
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=key,
        data=stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )

    print(f"✅ uploaded: s3://{MINIO_BUCKET}/{key}")

with DAG(
    dag_id="minio_upload_dag",
    start_date=datetime(2023,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["minio","pandas"],
) as dag:
    PythonOperator(task_id="upload_csv_to_minio", python_callable=upload_df_to_minio)
