from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import csv


def download_from_s3(**kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    key = "raw/customers.csv"
    bucket_name = "ecommerce-analytics-alejandro-dev"
    dest_path = "/opt/airflow/dags/customers.csv"   # usar o mesmo caminho que o load_to_postgres
    hook.get_key(key, bucket_name).download_file(dest_path)

def load_to_postgres(**kwargs):
    file_path = "/opt/airflow/dags/customers.csv"
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            signup_date DATE,
            country TEXT,
            state TEXT,
            city TEXT
        );
    """)
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # pula cabeçalho
        for row in reader:
            cur.execute(
                "INSERT INTO customers (customer_id, name, email, signup_date, country, state, city) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, signup_date = EXCLUDED.signup_date, country = EXCLUDED.country, state = EXCLUDED.state, city = EXCLUDED.city;",
                tuple(row)
            )
    conn.commit()
    cur.close()
    conn.close()

with DAG("s3_to_postgres_customers",
         start_date=datetime(2026, 1, 25),
         schedule_interval="@daily",
         catchup=False) as dag:

    extract = PythonOperator(
        task_id="download_customers",
        python_callable=download_from_s3,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_customers",
        python_callable=load_to_postgres,
        provide_context=True
    )

    extract >> load
