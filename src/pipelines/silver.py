import dlt, sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from utils.transforms import ORDER_RULES, quarantine_filter, standardize_orders
from pyspark.sql.functions import col, to_timestamp, to_date, upper, trim

QUARANTINE_FILTER = quarantine_filter()

@dlt.view(comment="Typed and standardized orders (not stored)")
def orders_typed():
    return standardize_orders(dlt.read_stream("bronze_orders"))

@dlt.table(comment="Clean, deduplicated orders",
           table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(ORDER_RULES)
def silver_orders():
    return (dlt.read_stream("orders_typed")
            .withWatermark("order_ts", "1 day")
            .dropDuplicatesWithinWatermark(["order_id"]))

@dlt.table(comment="Orders that failed quality rules, kept for investingation")
def silver_orders_quarantine():
    return (
        dlt.read_stream("orders_typed").filter(QUARANTINE_FILTER)
    )

@dlt.table(comment="Clean product reference data",
           table_properties={"quality": "silver"})

@dlt.expect_or_drop("valid_price", "price > 0")
def silver_products():
    return (
        dlt.read("bronze_products").dropDuplicates(["product_id"]).drop("_rescued_data")
    )
