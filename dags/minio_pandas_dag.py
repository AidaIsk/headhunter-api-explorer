import os, io
from minio import Minio
from datetime import datetime

ENDPOINT = os.environ["MINIO_ENDPOINT"]
ACCESS   = os.environ["MINIO_ACCESS_KEY"]
SECRET   = os.environ["MINIO_SECRET_KEY"]
BUCKET   = os.environ["MINIO_BUCKET"]

def upload_df_to_minio():
    df = pd.DataFrame({
        "id":[1,2,3],
        "name":["Alice","Bob","Charlie"],
        "role":["Data Engineer","Analyst","ML Engineer"]
    })
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    stream = io.BytesIO(csv_bytes)

    client = Minio(ENDPOINT, access_key=ACCESS, secret_key=SECRET, secure=False)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    key = f"test/{datetime.now():%Y/%m/%d}/pandas_test_{datetime.now():%H%M%S}.csv"
    client.put_object(BUCKET, key, stream, length=len(csv_bytes), content_type="text/csv")
    print(f"✅ uploaded: s3://{BUCKET}/{key}")
