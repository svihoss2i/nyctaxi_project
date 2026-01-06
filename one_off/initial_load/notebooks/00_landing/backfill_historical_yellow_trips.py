# Databricks notebook source
import urllib.request #help download any data using url
import shutil #help with high level file operations
import os #help with high level file operations and creating file

# COMMAND ----------

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

#lets read this now using urllib
response = urllib.request.urlopen(url)

#creating the directory where we are going to save the file 
dir_path = "/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/2025-01"

os.makedirs(dir_path, exist_ok=True)

loca_path = dir_path + "/yellow_tripdata_2025-01.parquet"

with open(loca_path, 'wb') as out_file:
  shutil.copyfileobj(response, out_file)


# COMMAND ----------

dates_to_process = ["2024-12", "2025-01", "2025-02", "2025-03", "2025-04", "2025-05"]

for date in dates_to_process:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"

    #lets read this now using urllib
    response = urllib.request.urlopen(url)

    #creating the directory where we are going to save the file 
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}"

    os.makedirs(dir_path, exist_ok=True)

    local_path = f"{dir_path }/yellow_tripdata_{date}.parquet"

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)