import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def valid_orders():
    """Business rule: cancelled orders don't count as revenue."""
    return dlt.read("silver_orders").filter("status != 'CANCELLED'")

@dlt.table(comment="Daily revenue by category",
           table_properties={"quality": "gold"},
           cluster_by=["order_date"])

def gold_daily_sales():
    products = dlt.read("silver_products").select("product_id", "category")
    return (valid_orders()
            .join(products, "product_id", "left")
            .groupBy("order_date", "category")
            .agg(F.countDistinct("order_id").alias("total_orders"),
                 F.sum("quantity").alias("unit_sold"),
                 F.round(F.sum("amount"), 2).alias("revenue"))
            .withColumn("avg_order_value",
                        F.round(F.col("revenue") / F.col("total_orders"), 2))
            )
    
@dlt.table(comment="Customer lifetime metrics",
           table_properties={"quality": "gold"})
def gold_customer_metrics():
    customers = dlt.read("silver_customers_current").select("customer_id", "name", "city")
    stats = (valid_orders()
             .groupBy("customer_id")
             .agg(F.count("order_id").alias("total_orders"),
                  F.round(F.sum("amount"), 2).alias("lifetime_value"),
                  F.min("order_date").alias("first_order_date"),
                  F.max("order_date").alias("last_order_date")))
    return (customers.join(stats, "customer_id", "left")
            .fillna(0, subset=["total_orders", "lifetime_value"]))
    

@dlt.table(comment="Revenue by city at time of order (point-in-time SCD2 join)",
           table_properties={"quality": "gold"})
def gold_revenue_by_city():
    w = Window.partitionBy("customer_id").orderBy("__START_AT")
    history = (dlt.read("silver_customers_scd2")
               .withColumn("valid_from",
                           F.when(F.row_number().over(w) == 1,
                                  F.lit("1900-01-01").cast("timestamp"))
                            .otherwise(F.col("__START_AT")))
               .withColumn("valid_to",
                           F.coalesce("__END_AT", F.lit("9999-12-31").cast("timestamp")))
               .select("customer_id", "city", "valid_from", "valid_to"))

    o, h = valid_orders().alias("o"), history.alias("h")
    return (o.join(h,
                   (F.col("o.customer_id") == F.col("h.customer_id")) &
                   (F.col("o.order_ts") >= F.col("h.valid_from")) &
                   (F.col("o.order_ts") <  F.col("h.valid_to")),
                   "inner")
            .groupBy("h.city")
            .agg(F.countDistinct("o.order_id").alias("total_orders"),
                 F.round(F.sum("o.amount"), 2).alias("revenue")))







