from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="transform_daily_revenue",
    start_date=datetime(2026, 2, 6),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # daily_revenue
    create_daily_revenue = PostgresOperator(
        task_id="create_daily_revenue",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS daily_revenue AS
            SELECT order_date::date AS day, SUM(total_value) AS revenue
            FROM sales_summary
            GROUP BY day;
        """
    )