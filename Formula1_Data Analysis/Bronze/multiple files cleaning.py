# Databricks notebook source
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# MAGIC %md ##Importing files

# COMMAND ----------

#just to try defining schema (another way of importing json with defined schema)
laptime_schema = StructType(
    fields=[
        StructField("race_id", IntegerType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("laps", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md ###Importing csv files (laptime)

# COMMAND ----------

laptimes_df = spark.read.option("header", True).schema(laptime_schema).csv("/mnt/avkrgadls/Formula1DA/raw/lap_times_split*.csv")

# COMMAND ----------

qual_df = spark.read.option("header", True).json("/mnt/avkrgadls/Formula1DA/raw/qualifying_split_*.json",multiLine=True)

# COMMAND ----------

# MAGIC %md ###cleaning data

# COMMAND ----------

laptimes_cleaned_df = laptimes_df.withColumn("ingestion_date", lit(datetime.now()))

# COMMAND ----------

qual_cleaned_df = (
    qual_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("qualifyId", "qualify_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumn("ingestion_date", lit(datetime.now()))
)

# COMMAND ----------

# MAGIC %md ##Saving to DBFS

# COMMAND ----------

laptimes_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.laptimes")

# COMMAND ----------

qual_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.qualifying")