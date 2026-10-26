import dlt
from pyspark.sql.functions import col, to_timestamp

@dlt.view(comment="Typed customer CDC feed")
def customers_cdc():
    return (dlt.read_stream("bronze_customers")
            .withColumn("update_at", to_timestamp("updated_at"))
            )

dlt.create_streaming_table(
    name="silver_customers_scd2",
    comment="Customer history (SCD Type 2)",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="silver_customers_scd2",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2,
    except_column_list=["operation", "_rescued_data", "_source_file", "_ingested_at"]
)

@dlt.table(comment="Current customer snapshot for joins")
def silver_customers_current():
    return dlt.read("silver_customers_scd2").filter("__END_AT is NULL")



