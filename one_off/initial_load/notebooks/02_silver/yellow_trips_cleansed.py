# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")
df.display()

# COMMAND ----------

#get the minimum and maximum picku time
from pyspark.sql.functions import min, max
df.agg(max("tpep_pickup_datetime"), min("tpep_pickup_datetime")).display()

# COMMAND ----------

df = df.filter("tpep_pickup_datetime >= '2025-01-01' AND tpep_pickup_datetime <= '2025-07-01'")

df.display()


# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 4, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor"),

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    # timestamp_diff expects column expressions, not df.column
    timestamp_diff(
        'MINUTE', 
        col("tpep_pickup_datetime"), 
        col("tpep_dropoff_datetime")).alias("trip_duration"),
    "passenger_count",
    "trip_distance",

    when(col("RatecodeID") == 1, "Standard Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated Fare")
        .when(col("RatecodeID") == 6, "Group Ride")
        .otherwise("Unknown")
        .alias("rate_type"),

    col("store_and_fwd_flag"),
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Unknown")
        .alias("payment_type"),

    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tolls_amount"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("congestion_surcharge"),
    col("Airport_fee").alias("airport_fee"),
    col("cbd_congestion_fee"),
    col("processed_timestamp")
)

df.display()

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")