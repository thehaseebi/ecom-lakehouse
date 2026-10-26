from pyspark.sql import functions as F

ORDER_RULES = {
    "valid_order_id":  "order_id IS NOT NULL",
    "valid_customer":  "customer_id IS NOT NULL",
    "positive_amount": "amount > 0",
    "valid_quantity":  "quantity > 0",
}

def quarantine_filter(rules=ORDER_RULES):
    return "NOT ({})".format(" AND ".join(rules.values()))

def standardize_orders(df):
    return (df.withColumn("order_ts", F.to_timestamp("order_ts"))
              .withColumn("order_date", F.to_date("order_ts"))
              .withColumn("status", F.upper(F.trim(F.col("status"))))
              .drop("_rescued_data"))