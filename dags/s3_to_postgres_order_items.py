from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import csv


def download_order_items_from_s3(**kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    key = "raw/order_items.csv"
    bucket_name = "ecommerce-analytics-alejandro-dev"
    dest_path = "/opt/airflow/dags/order_items.csv"   # usar o mesmo caminho que o load_to_postgres
    hook.get_key(key, bucket_name).download_file(dest_path)

def load_to_order_items_postgres(**kwargs):
    file_path = "/opt/airflow/dags/order_items.csv"
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id TEXT PRIMARY KEY,
            order_id TEXT REFERENCES orders(order_id),
            products_id TEXT REFERENCES products(products_id),
            quantity INT,
            unit_price NUMERIC,
            discount NUMERIC
        );
    """)
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # pula cabeçalho
        for row in reader:
            cur.execute(
                "INSERT INTO order_items (order_item_id, order_id, products_id, quantity, unit_price, discount) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (order_item_id) DO UPDATE SET order_id = EXCLUDED.order_id, products_id = EXCLUDED.products_id, quantity = EXCLUDED.quantity, unit_price = EXCLUDED.unit_price, discount = EXCLUDED.discount;",
                tuple(row)
            )
    conn.commit()
    cur.close()
    conn.close()

with DAG("s3_to_postgres_order_items",
         start_date=datetime(2026, 1, 25),
         schedule_interval="@daily",
         catchup=False) as dag:

    extract = PythonOperator(
        task_id="download_order_items",
        python_callable=download_order_items_from_s3,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_order_items",
        python_callable=load_to_order_items_postgres,
        provide_context=True
    )

    extract >> load
