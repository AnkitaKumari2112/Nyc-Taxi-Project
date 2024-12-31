# Databricks notebook source
# MAGIC %run "../Mounting_delta_lake"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONFIGURATION OF SERVICE PRINCIPLE, STORAGE ACCOUNT, DATABRICKS

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.--storage-account--.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.--storage-account--.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.--storage-account--.dfs.core.windows.net", "--application-id--")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.--storage-account--.dfs.core.windows.net", "--secret-id--")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.--storage-account--.dfs.core.windows.net", "https://login.microsoftonline.com/--directory-id--/oauth2/token")
# MAGIC
# MAGIC Replace
# MAGIC - --secret-id-- with the Databricks secret id value. 
# MAGIC - --storage-account-- with the name of the Azure storage account. 
# MAGIC - --application-id-- with the Application (client) ID for the Microsoft Entra ID application. 
# MAGIC - --directory-id-- with the Directory (tenant) ID for the Microsoft Entra ID application. 

# COMMAND ----------

# MAGIC %md
# MAGIC Testing connection

# COMMAND ----------

dbutils.fs.ls("abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING DATAFRAME BY READING PARQUET FILE FROM STORAGE ACCOUNT

# COMMAND ----------

# MAGIC %md
# MAGIC Import all the libraries of pyspark at the beigining. (function and datatype)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from file

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
                 .option("inferSchema", "true")\
                    .option("header", "true")\
                        .load("abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_type/trip_type.csv")
df_trip_type.printSchema()

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                .option("inferSchema", "true")\
                .option("header", "true")\
                .load("abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_zone/taxi_zone_lookup.csv")
df_trip_zone.printSchema()
df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Recurisevly reading teh file inside nested folders

# COMMAND ----------

df_green_taxi_data = spark.read.format("parquet")\
                        .option("inferSchema", "true")\
                        .option("header", "true")\
                        .option("recursiveFileLookup", "true")\
                        .load("abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_data")
df_green_taxi_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Create own data frame schema instead of using inferschema option

# COMMAND ----------

df_green_taxi_data_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),  
    StructField("RatecodeID", DoubleType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", IntegerType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", DoubleType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

df_green_taxi = spark.read.format("parquet")\
                    .schema(df_green_taxi_data_schema)\
                        .option("header", "true")\
                            .option("recursiveFileLookup", "true")\
                            .load("abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_data/")
df_green_taxi.printSchema()
df_green_taxi.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming the column name

# COMMAND ----------

df_trip_type_rename = df_trip_type.withColumnRenamed("description","trip_description")
df_trip_type_rename.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC SPlitting a column into multiple column

# COMMAND ----------

df_trip_zone_split_column = df_trip_zone.withColumn("Zone1", split(col("Zone"), "/")[0])\
                                        .withColumn("Zone2", split(col("Zone"), "/")[1])
df_trip_zone_new = df_trip_zone_split_column.select("LocationID","Borough","Zone1","Zone2","service_zone")
df_trip_zone_new.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC creating 2 other column [date,time] uisng timestamp column value.

# COMMAND ----------

df_green_taxi_column = df_green_taxi_data.withColumn("lpep_pickup_date_time",date_format(col("lpep_pickup_datetime"),"yyyy-MM-dd HH:mm:ss"))\
                            .withColumn("lpep_dropoff_date_time",date_format(col("lpep_dropoff_datetime"),"yyyy-MM-dd HH:mm:ss"))

df_green_taxi_final = df_green_taxi_column.select("VendorID","lpep_pickup_date_time","lpep_dropoff_date_time","store_and_fwd_flag","RatecodeID","PULocationID","DOLocationID","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","ehail_fee","improvement_surcharge","total_amount","payment_type","trip_type","congestion_surcharge")

df_green_taxi_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER DATA TRANSFORMATION WRITING DATA INTO <--container_name--> FOLDER

# COMMAND ----------

df_trip_type_rename.write.format("parquet")\
                .mode("overwrite")\
                    .option("path","abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_type")\
                        .save()

# COMMAND ----------

df_trip_zone_new.write.format("parquet")\
                .mode("overwrite")\
                       .option("path","abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_zone")\
                           .save()

# COMMAND ----------

df_green_taxi_final.write.format("parquet")\
    .mode("overwrite")\
        .option("path","abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_data/")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC for validating wether the complete data has been written in <--container_name--> folder or not.

# COMMAND ----------

spark.read.parquet("abfss://<--container_name-->@<--storage_account-->.dfs.core.windows.net/trip_data/").count()