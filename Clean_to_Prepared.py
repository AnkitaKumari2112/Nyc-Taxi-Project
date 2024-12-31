# Databricks notebook source
# MAGIC %md
# MAGIC Trnsformation is done till <--container_name--> layer only. AFter <--container_name--> layer we are just creating dataframes in databricks in delta format, which can be used for data analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading and writing and creating delta tables.

# COMMAND ----------

# MAGIC %run "../Mounting_delta_lake"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating database for storing delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE nyc_taxi_data CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE nyc_taxi_data;
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting parquet file from <--container_name--> layer to delta table in <--container_name--> layer

# COMMAND ----------

df_taxi_zone = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/<--storage_account-->/<--container_name-->/trip_zone")

# COMMAND ----------

df_taxi_zone.write.format("delta").mode("overwrite").option("path", "/mnt/<--storage_account-->/<--container_name-->/trip_zone").option("mergeSchema", "true").saveAsTable("nyc_taxi_data.taxi_zone")

# COMMAND ----------

df_taxi_trip_type = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/<--storage_account-->/<--container_name-->/trip_type")

# COMMAND ----------

df_taxi_trip_type.write.format("delta").mode("overwrite").option("path", "/mnt/<--storage_account-->/<--container_name-->/trip_type").saveAsTable("nyc_taxi_data.trip_type")

# COMMAND ----------

df_taxi_trip_data = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/<--storage_account-->/<--container_name-->/trip_data")

# COMMAND ----------

df_taxi_trip_data.write.format("delta").mode("overwrite").option("path", "/mnt/<--storage_account-->/<--container_name-->/trip_data").saveAsTable("nyc_taxi_data.trip_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc_taxi_data.trip_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Learnig versioning on delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC update nyc_taxi_data.taxi_zone set Borough = 'EXR' where LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc_taxi_data.taxi_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from nyc_taxi_data.taxi_zone where LocationID = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### To check table versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyc_taxi_data.taxi_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel: Bring back the data which was deleted or updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE nyc_taxi_data.taxi_zone VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_taxi_data.taxi_zone;