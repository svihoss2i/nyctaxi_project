# Databricks notebook source
import urllib.request
import shutil
import os

#target URL of the publuc csv file to download
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

#open a connection to the remote url and etch the parquet file as a strem
response = urllib.request.urlopen(url)

#create the destination directory for storing the downloaded parquet file
dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
os.makedirs(dir_path, exist_ok=True)

#define the full local path (including filename) where the file will be saved
local_path = f"{dir_path}/taxi_zone_lookup.csv"

#open the local file in binary mode and write the contents of the remote file to it
with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)

# COMMAND ----------

from pysspark.sql.functions import current_timestamp

df = spark.read.format("parquet").load("/volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*") #the start at the end of the path is a wildcard which will match all files in the directory and append them together as dataframe. 

df = df.withColumn("processed_timestamp", current_timestamp())

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw"))


# COMMAND ----------

