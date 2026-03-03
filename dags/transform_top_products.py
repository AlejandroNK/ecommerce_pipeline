from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="transform_top_products",
    start_date=datetime(2026, 2, 6),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # top_products
    create_top_products = PostgresOperator(
        task_id="create_top_products",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS top_products AS
            SELECT products_id, product_name, SUM(quantity) AS total_sold
            FROM sales_summary
            GROUP BY products_id, product_name
            ORDER BY total_sold DESC;
        """
    )