from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="transform_top_customers",
    start_date=datetime(2026, 2, 6),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # top_customers
    create_top_customers = PostgresOperator(
        task_id="create_top_customers",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS top_customers AS
            SELECT customer_id, customer_name, SUM(total_value) AS total_spent
            FROM sales_summary
            GROUP BY customer_id, customer_name
            ORDER BY total_spent DESC;
        """
    )