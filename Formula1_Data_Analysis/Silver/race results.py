# Databricks notebook source
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# MAGIC %md ##getting race results
# MAGIC - left join results with constructors, driver
# MAGIC - left join races with circuit to get circuit id
# MAGIC - left join 1st table with 2nd table

# COMMAND ----------

race_results_df = spark.sql(
    """
    select race_year, race_name, race_date, circuit_location, d.name as driver_name, d.number as driver_number, d.nationality as driver_nationality, c.name as team, re.grid as grid, re.fastest_lap as fastest_lap, re.time as race_time, re.points as points,re.position as position
    from 
    formula1db.bronze.results re left join formula1db.bronze.constructors c on re.constructor_id = c.constructorId
    left join formula1db.bronze.drivers d on re.driver_id = d.driver_id
    left join 
        (select r.race_id, r.race_year as race_year, r.name as race_name, r.race_timestamp as race_date, c.location as circuit_location 
        from 
        formula1db.bronze.races r left join formula1db.bronze.circuits c on r.circuit_id = c.circuit_id) rc 
    on re.race_id = rc.race_id
    """).withColumn("ingestion_date",lit(datetime.now()))

# COMMAND ----------

race_results_df.filter("race_year == 2020 and race_name like 'Abu%' ").orderBy(desc("points")).display()

# COMMAND ----------

race_results_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.silver.race_results")