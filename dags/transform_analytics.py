from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="transform_analytics",
    start_date=datetime(2026, 2, 6),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Sales Summary
    create_sales_summary = PostgresOperator(
        task_id="create_sales_summary",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS sales_summary AS
            SELECT 
                o.order_id,
                o.order_date,
                c.customer_id,
                c.name AS customer_name,
                p.products_id,
                p.name AS product_name,
                p.category,
                oi.quantity,
                oi.unit_price,
                oi.discount,
                (oi.quantity * oi.unit_price - oi.discount) AS total_value
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.products_id = p.products_id;
        """
    )

    # Customer Activity
    create_customer_activity = PostgresOperator(
        task_id="create_customer_activity",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS customer_activity AS
            SELECT 
                c.customer_id,
                c.name,
                COUNT(o.order_id) AS total_orders,
                SUM(oi.quantity * oi.unit_price - oi.discount) AS total_spent,
                AVG(oi.quantity * oi.unit_price - oi.discount) AS avg_order_value
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY c.customer_id, c.name;
        """
    )

    # Product Performance
    create_product_performance = PostgresOperator(
        task_id="create_product_performance",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS product_performance AS
            SELECT 
                p.products_id,
                p.name,
                p.category,
                SUM(oi.quantity) AS total_quantity_sold,
                SUM(oi.quantity * oi.unit_price - oi.discount) AS total_revenue,
                AVG(oi.discount) AS avg_discount
            FROM products p
            LEFT JOIN order_items oi ON p.products_id = oi.products_id
            GROUP BY p.products_id, p.name, p.category;
        """
    )

    # Dependências: rodar em sequência
    create_sales_summary >> create_customer_activity >> create_product_performance