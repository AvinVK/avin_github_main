# Databricks notebook source
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# MAGIC %md ##calculating race results
# MAGIC

# COMMAND ----------

calc_race_results_df = spark.sql(
    """
    select r.year as race_year, d.name as driver_name,c.name as team, re.points as points, re.position as position, 11-re.position as calc_points
    from 
    formula1db.bronze.results re 
    left join formula1db.bronze.constructors c on re.constructor_id = c.constructorId
    left join formula1db.bronze.drivers d on re.driver_id = d.driver_id
    left join formula1db.bronze.races r on r.race_id = re.race_id
    where position <= 10
    """)

# COMMAND ----------

# MAGIC %md ##saving to dbfs

# COMMAND ----------

calc_race_results_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.silver.calc_race_results")