# Databricks notebook source
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md ##Importing files

# COMMAND ----------

# MAGIC %md ###Importing csv files (circuits, races)

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv("/mnt/avkrgadls/Formula1DA/raw/circuits.csv")
races_df = spark.read.option("header", True).csv("/mnt/avkrgadls/Formula1DA/raw/races.csv")

# COMMAND ----------

# MAGIC %md ###cleaning data

# COMMAND ----------

circuits_cleaned_df = (
    circuits_df.withColumnRenamed("circuitRef", "circuit_ref")
    .withColumn("circuit_id", col("circuitId").cast("integer"))
    .withColumn("latitude", col("lat").cast("double"))
    .withColumn("longitude", col("lng").cast("double"))
    .withColumn("altitude", col("alt").cast("integer"))
    .withColumn("ingestion_date",lit(datetime.now()))
    .drop("url","circuitId","lat","long","alt")
)

# COMMAND ----------

races_cleaned_df = (
    races_df.withColumn("race_id", col("raceId").cast("integer"))
    .withColumn("race_year", col("year").cast("integer"))
    .withColumn("circuit_id", col("circuitID").cast("integer"))
    .withColumn("round", col("round").cast("integer"))
    .withColumn("ingestion_date", lit(datetime.now()))
    .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd hh:mm:ss"))
    .drop("url", "circuitId", "raceId","date","time")
)

# COMMAND ----------

# MAGIC %md ##Saving to DBFS

# COMMAND ----------

circuits_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.circuits")

# COMMAND ----------

races_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.races")