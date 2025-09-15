FROM apache/airflow:2.9.2-python3.10

USER airflow


RUN pip install --no-cache-dir requests pandas psycopg2-binary duckdb