# Databricks notebook source
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# MAGIC %md ##Importing files

# COMMAND ----------

# MAGIC %md ###Importing json files (constructors, drivers, results, pitstop)

# COMMAND ----------

#just to try defining schema (another way of importing json with defined schema)
results_schema = StructType(
    fields=[
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("laps", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", FloatType(), True),
        StructField("statusId", StringType(), True),
    ]
)

# COMMAND ----------

#pitstop json is a multiline json file!
const_df = spark.read.option("header", True).json("/mnt/avkrgadls/Formula1DA/raw/constructors.json")
drivers_df = spark.read.option("header", True).json("/mnt/avkrgadls/Formula1DA/raw/drivers.json")
results_df = spark.read.schema(results_schema).option("header", True).json("/mnt/avkrgadls/Formula1DA/raw/results.json")
pitstop_df = spark.read.option("header", True).json("/mnt/avkrgadls/Formula1DA/raw/pit_stops.json",multiLine=True)

# COMMAND ----------

# MAGIC %md ###cleaning data

# COMMAND ----------

const_cleaned_df = (
    const_df.withColumn("constructor_id", col("constructorId").cast("integer"))
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("ingestion_date",lit(datetime.now()))
    .drop("url")
)

# COMMAND ----------

drivers_cleaned_df = (
    drivers_df.withColumnRenamed("driverRef", "driver_ref")
    .withColumn("driver_id", col("driverId").cast("integer"))
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
    .withColumn("ingestion_date", lit(datetime.now()))
    .withColumn(
        "number", when(col("number") == "\\N", lit(None)).otherwise(col("number"))
    )
    .drop("url", "driverId")
)

# COMMAND ----------

results_cleaned_df = (
    results_df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
    .withColumn("ingestion_date", lit(datetime.now()))
).drop("url")

# COMMAND ----------

pitstop_cleaned_df = (
    pitstop_df.withColumnRenamed("raceId", "race_id")
    .withColumn("driver_id", col("driverId").cast("integer"))
    .withColumn("ingestion_date", lit(datetime.now()))
    .drop("url", "driverId")
)

# COMMAND ----------

# MAGIC %md ##Saving to DBFS

# COMMAND ----------

const_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.constructors")
drivers_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.drivers")
results_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.results")
pitstop_cleaned_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.bronze.pitstops")