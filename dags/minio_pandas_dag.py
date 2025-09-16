from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def make_and_save_csv():
    # 1. Создаём простую таблицу
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "role": ["Data Engineer", "Analyst", "ML Engineer"]
    })

    # 2. Сохраняем файл в папку MinIO 
    path = "/test-bucket/pandas_test.csv"
    df.to_csv(path, index=False)

    print(f"✅ CSV сохранён: {path}")


# Определение DAG
with DAG(
    dag_id="minio_pandas_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,   
    catchup=False,
    tags=["test", "minio", "pandas"],
) as dag:

    save_csv = PythonOperator(
        task_id="save_csv_with_pandas",
        python_callable=make_and_save_csv,
    )
