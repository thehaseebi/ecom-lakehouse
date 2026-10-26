import pytest
from pyspark.sql import SparkSession
from utils.transforms import ORDER_RULES, quarantine_filter, standardize_orders

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_standardize_orders(spark):
    df = spark.createDataFrame(
        [("o1", " shipped ", "2026-10-26T10:00:00")],
        ["order_id", "status", "order_ts"])
    row = standardize_orders(df).collect()[0]
    assert row.status == "SHIPPED"
    assert str(row.order_date) == "2026-10-26"

def test_quality_rules_split_good_and_bad(spark):
    df = spark.createDataFrame(
        [("o1", "C1", 10.0, 1),     # good
         ("o2", None, 10.0, 1),     # null customer
         ("o3", "C1", -5.0, 1)],    # negative amount
        ["order_id", "customer_id", "amount", "quantity"])
    good = " AND ".join(ORDER_RULES.values())
    assert df.filter(good).count() == 1
    assert df.filter(quarantine_filter()).count() == 2