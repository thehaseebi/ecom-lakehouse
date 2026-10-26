import dlt
from pyspark.sql.functions import col, current_timestamp

RAW = spark.conf.get("raw_path")

def ingest(source):
    """Reuseable Auto Loader reader: incremental, schema-aware."""
    return(spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.inferColumnTypes", "true")
           .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
           .load(f"{RAW}/{source}")
           .withColumn("_source_file", col("_metadata.file_path"))
           .withColumn("_ingested_at", current_timestamp())   
        )
    
@dlt.table(comment="Raw orders, append-only",
           table_properties={"quality": "bronze"})
def bronze_orders():
    return ingest("orders")

@dlt.table(comment="Raw customer CDC events",
           table_properties={"quality": "bronze"})
def bronze_customers():
    return ingest("customers")

@dlt.table(comment="Raw product reference data",
           table_properties={"quality": "bronze"})
def bronze_products():
    return ingest("products")
