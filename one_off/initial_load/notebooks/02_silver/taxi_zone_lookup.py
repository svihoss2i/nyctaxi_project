# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType



# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")
df.display()

# COMMAND ----------

# lit(None).cast(TimestampType()).alias("expiration_date") is used to add a column named "expiration_date"
# with all values set to null (None) and explicitly cast to TimestampType.
# This is useful for schema consistency or future updates where expiration_date may be populated.
df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"), 
    col("Borough").alias("borough"), 
    col("Zone").alias("zone"), 
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("expiration_date")
)
df.display()

# COMMAND ----------

#saving the dataframe as a delta table
df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

spark.sql("select * from nyctaxi.02_silver.taxi_zone_lookup").display()