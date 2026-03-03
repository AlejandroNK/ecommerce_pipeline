from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import csv


def download_products_from_s3(**kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    key = "raw/products.csv"
    bucket_name = "ecommerce-analytics-alejandro-dev"
    dest_path = "/opt/airflow/dags/products.csv"   # usar o mesmo caminho que o load_to_postgres
    hook.get_key(key, bucket_name).download_file(dest_path)

def load_to_products_postgres(**kwargs):
    file_path = "/opt/airflow/dags/products.csv"
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            products_id TEXT PRIMARY KEY,
            sku TEXT,
            name TEXT,
            category TEXT,
            price NUMERIC
        );
    """)
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # pula cabeçalho
        for row in reader:
            cur.execute(
                "INSERT INTO products (products_id, sku, name, category, price) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (products_id) DO UPDATE SET sku = EXCLUDED.sku, name = EXCLUDED.name, category = EXCLUDED.category, price = EXCLUDED.price;",
                tuple(row)
            )
    conn.commit()
    cur.close()
    conn.close()

with DAG("s3_to_postgres_products",
         start_date=datetime(2026, 1, 25),
         schedule_interval="@daily",
         catchup=False) as dag:

    extract = PythonOperator(
        task_id="download_products",
        python_callable=download_products_from_s3,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_products",
        python_callable=load_to_products_postgres,
        provide_context=True
    )

    extract >> load
