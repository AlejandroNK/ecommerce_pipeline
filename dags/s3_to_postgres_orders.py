from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import csv


def download_orders_from_s3(**kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    key = "raw/orders.csv"
    bucket_name = "ecommerce-analytics-alejandro-dev"
    dest_path = "/opt/airflow/dags/orders.csv"   # usar o mesmo caminho que o load_to_postgres
    hook.get_key(key, bucket_name).download_file(dest_path)

def load_to_orders_postgres(**kwargs):
    file_path = "/opt/airflow/dags/orders.csv"
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT REFERENCES customers(customer_id),
            order_date TIMESTAMP,
            status TEXT,
            payment_method TEXT
        );
    """)
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # pula cabeçalho
        for row in reader:
            cur.execute(
                "INSERT INTO orders (order_id, customer_id, order_date, status, payment_method) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (order_id) DO UPDATE SET customer_id = EXCLUDED.customer_id, order_date = EXCLUDED.order_date, status = EXCLUDED.status, payment_method = EXCLUDED.payment_method;",
                tuple(row)
            )
    conn.commit()
    cur.close()
    conn.close()

with DAG("s3_to_postgres_orders",
         start_date=datetime(2026, 1, 25),
         schedule_interval="@daily",
         catchup=False) as dag:

    extract = PythonOperator(
        task_id="download_orders",
        python_callable=download_orders_from_s3,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_orders",
        python_callable=load_to_orders_postgres,
        provide_context=True
    )

    extract >> load
