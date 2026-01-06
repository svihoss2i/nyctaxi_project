# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Which vendorr makes the most revenue?

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

df.display()

# COMMAND ----------

df.\
    groupBy("vendor").\
    agg(
        round(sum("total_amount"), 2).alias("total_revenue")
        ).\
    orderBy("total_revenue", ascending=False).\
    display()


# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

# Replace 'vendor' with the actual column name for vendor if different
vendor_revenue = df.groupBy("vendor").agg(spark_sum("total_amount").alias("total_revenue"))
top_vendor = vendor_revenue.orderBy(col("total_revenue").desc()).limit(1)

display(top_vendor)

# COMMAND ----------

# MAGIC %md
# MAGIC What is the most popular pickup borough?

# COMMAND ----------

df.\
    groupBy("pu_borough").\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

from pyspark.sql.functions import col

pickup_borough_counts = df.groupBy("pu_borough").count().orderBy(col("count").desc()).limit(1)
display(pickup_borough_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC what is the most common journey (borough to borough)?

# COMMAND ----------

df.\
    groupBy("pu_borough", "do_borough").\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

df.\
    groupBy(concat("pu_borough", lit(" ->"), lit("do_borough")).alias("journey")).\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

from pyspark.sql.functions import col, count

journey_counts = df.groupBy("pu_borough", "do_borough").agg(count("*").alias("journey_count"))
most_common_journey = journey_counts.orderBy(col("journey_count").desc()).limit(1)

display(most_common_journey)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a time series chart showing the number of trips and total revenue per day

# COMMAND ----------

df2 = spark.read.table("nyctaxi.03_gold.daily_trip_summary")

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC The visualization in the previous cell (Cell 2) displays the most common journey between pickup and dropoff boroughs in your taxi dataset. It shows the pair of pickup (pu_borough) and dropoff (do_borough) boroughs that had the highest number of trips, along with the count of those trips. This helps you quickly identify the most frequently traveled route in your data.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import to_date, count, sum as spark_sum

daily_stats = df.groupBy(to_date("tpep_pickup_datetime").alias("date")) \
    .agg(
        count("*").alias("number_of_trips"),
        spark_sum("total_amount").alias("total_revenue")
    ) \
    .orderBy("date")

display(daily_stats)