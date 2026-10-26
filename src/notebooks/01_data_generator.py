# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import json, random, uuid
from datetime import datetime, timedelta
from faker import Faker
import os
import random

dbutils.widgets.text("schema", "ecom")
SCHEMA = dbutils.widgets.get("schema")
S = f"workspace.{SCHEMA}"
BASE = f"/Volumes/workspace/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {S}.raw")


fake = Faker()
NUM_CUSTOMERS = 500
CATEGORIES = ["Electronics", "Fashion", "Home", "Books", "Sports"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]

def write_batch(folder, records):
    folder_path = f"{BASE}/{folder}"
    os.makedirs(folder_path, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{BASE}/{folder}/{folder}_{ts}.json"
    with open(path, 'w') as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        print(f"Wrote {len(records)} records -> {path}")

def gen_products():
    rng = random.Random(76)
    return [{
        "product_id": f"P{i:03d}",
        "category": (cat := rng.choice(CATEGORIES)),
        "name": f"{cat} item {i}",
        "price": round(rng.uniform(5, 200), 2)
    } for i in range(1, 51)]

def gen_customers(first_run):
    now = datetime.now().isoformat()
    if first_run:
        ids, op = range(1, NUM_CUSTOMERS + 1), 'INSERT'
    else:
        ids, op = random.sample(range(1, NUM_CUSTOMERS + 1), 20), "UPDATE"
    return [{
        "customer_id": f"C{i:04d}",
        "name": f"Customer {i}",
        "email": fake.email(),
        "city": fake.city(),
        "operation": op,
        "updated_at": now
    } for i in ids]

def gen_orders(products, n=1000):
    records = []
    for _ in range(n):
        p = random.choice(products)
        qty = random.randint(1, 5)
        records.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": f"C{random.randint(1, NUM_CUSTOMERS):04d}",
            "product_id": p["product_id"],
            "quantity": qty,
            "amount": round(p["price"] * qty, 2),
            "status": random.choice(STATUSES),
            "order_ts": (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat()
        })
    
    for r in random.sample(records, int(n * 0.03)):
        issue = random.choice(["negative", "null", "duplicate"])
        if issue == "negative":
            r["amount"] = -r["amount"]
        elif issue == "null":
            r["customer_id"] = None
        else:
            records.append(dict(r))
    return records


# ---- RUN ----
products = gen_products()
first_run = len(dbutils.fs.ls(f"{BASE}/customers")) == 0

if first_run:
    write_batch("products", products)
write_batch("customers", gen_customers(first_run))
write_batch("orders", gen_orders(products))

# COMMAND ----------

orders = spark.read.json(f"{BASE}/orders")
print("Total orders:", orders.count())
print("Bad amounts:", orders.filter("amount <= 0").count())
print("Null customers:", orders.filter("customer_id IS NULL").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_versions,
# MAGIC   COUNT_IF(__END_AT IS NULL) AS current_rows
# MAGIC FROM workspace.ecom.silver_customers_scd2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT ROUND(SUM(amount), 2) FROM workspace.ecom.silver_orders
# MAGIC      WHERE status != 'CANCELLED')                                  AS silver_revenue,
# MAGIC   (SELECT ROUND(SUM(revenue), 2) FROM workspace.ecom.gold_daily_sales)     AS gold_daily,
# MAGIC   (SELECT ROUND(SUM(revenue), 2) FROM workspace.ecom.gold_revenue_by_city) AS gold_by_city;