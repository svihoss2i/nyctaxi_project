# Databricks notebook source
from pyspark.sql.functions import current_timestamp




# COMMAND ----------

df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*") #the start at the end of the path is a wildcard which will match all files in the directory and append them together as dataframe. 

df.display()


# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())



# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").display()

# COMMAND ----------

