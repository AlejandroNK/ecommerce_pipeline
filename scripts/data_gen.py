import random, uuid
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Configuração do banco
PG_CONN = dict(host="localhost", dbname="ecommerce", user="postgres", password="postgres")
OUTPUT_DIR = "output_batches"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Funções auxiliares
def rand_date(days=90):
    return datetime.now() - timedelta(days=random.randint(0, days))

def gen_customers(n=50):
    rows = []
    for i in range(n):
        rows.append({
            "customer_id": str(uuid.uuid4()),
            "name": f"Cliente {i}",
            "email": f"cliente{i}@exemplo.com",
            "signup_date": (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
            "country": "Brasil",
            "state": random.choice(["PR","SP","RJ","RS","SC","MG"]),
            "city": random.choice(["Curitiba","São Paulo","Rio","Porto Alegre","Florianópolis","Belo Horizonte"]),
        })
    return pd.DataFrame(rows)

def gen_products(n=20):
    cats = ["Eletrônicos","Casa","Esporte","Moda"]
    rows = []
    for i in range(n):
        price = round(random.uniform(20, 1500), 2)
        rows.append({
            "product_id": str(uuid.uuid4()),
            "sku": f"SKU-{i:05d}",
            "name": f"Produto {i}",
            "category": random.choice(cats),
            "price": price,
        })
    return pd.DataFrame(rows)

def gen_orders(customers_df, products_df, n=100):
    orders, items = [], []
    for i in range(n):
        c = customers_df.sample(1).iloc[0]
        order_id = str(uuid.uuid4())
        od = rand_date()
        orders.append({
            "order_id": order_id,
            "customer_id": c["customer_id"],
            "order_date": od,
            "status": random.choice(["paid","cancelled","refunded","pending"]),
            "payment_method": random.choice(["card","pix","boleto","paypal"])
        })
        for _ in range(random.randint(1,3)):
            p = products_df.sample(1).iloc[0]
            items.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": p["product_id"],
                "quantity": random.randint(1,5),
                "unit_price": float(p["price"]),
                "discount": round(random.choice([0,0,0, p["price"]*0.05]), 2)
            })
    return pd.DataFrame(orders), pd.DataFrame(items)

def insert_df(conn, df, table):
    cols = ",".join(df.columns)
    vals = ",".join(["%s"] * len(df.columns))
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(f"insert into {table} ({cols}) values ({vals})", tuple(row))
    conn.commit()

def export(df, name):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{OUTPUT_DIR}/{name}.parquet")
    df.to_csv(f"{OUTPUT_DIR}/{name}.csv", index=False)

if __name__ == "__main__":
    customers = gen_customers(50)
    products = gen_products(20)
    orders, order_items = gen_orders(customers, products, 100)

    conn = psycopg2.connect(**PG_CONN)
    insert_df(conn, customers, "customers")
    insert_df(conn, products, "products")
    insert_df(conn, orders, "orders")
    insert_df(conn, order_items, "order_items")

    export(customers, "customers")
    export(products, "products")
    export(orders, "orders")
    export(order_items, "order_items")

    print("✅ Dados gerados e inseridos com sucesso!")
