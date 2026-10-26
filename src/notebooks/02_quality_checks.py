# Databricks notebook source
S = "workspace.ecom"

def scalar(query):
    return spark.sql(query).collect()[0][0]

checks = {
    "revenue_reconciles": scalar(f"""
        SELECT ABS(
          (SELECT SUM(amount) FROM {S}.silver_orders WHERE status != 'CANCELLED') -
          (SELECT SUM(revenue) FROM {S}.gold_daily_sales)
        ) < 1"""),

    "one_current_row_per_customer": scalar(f"""
        SELECT COUNT(*) = 0 FROM (
          SELECT customer_id FROM {S}.silver_customers_scd2
          WHERE __END_AT IS NULL
          GROUP BY customer_id HAVING COUNT(*) > 1)"""),

    "quarantine_rate_below_5pct": scalar(f"""
        SELECT (SELECT COUNT(*) FROM {S}.silver_orders_quarantine) /
               (SELECT COUNT(*) FROM {S}.bronze_orders) < 0.001"""),

    "data_is_fresh": scalar(f"""
        SELECT MAX(_ingested_at) > current_timestamp() - INTERVAL 2 HOURS
        FROM {S}.bronze_orders"""),
}

for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}  {name}")

failed = [name for name, passed in checks.items() if not passed]
if failed:
    raise Exception(f"Data quality checks failed: {failed}")