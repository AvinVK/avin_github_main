# Databricks notebook source
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# MAGIC %md ##getting driver standing
# MAGIC

# COMMAND ----------

driver_standing_df = spark.sql(
    """
    select race_year, driver_name, driver_nationality, team, sum(points) as total_points, 
    sum(
        case
        when position = 1 then 1 else 0 end) 
        as total_wins
    from 
    formula1db.silver.race_results 
    group by race_year, driver_name, driver_nationality, team
    """)

# COMMAND ----------

driver_standing_ranked_df = spark.sql(
    """
    select *, dense_rank() over(partition by race_year order by total_points desc, total_wins asc) as driver_rank  
    from {table}
    """,
    table = driver_standing_df,
)

# COMMAND ----------

# MAGIC %md ##getting team standing

# COMMAND ----------

team_standing_df = spark.sql(
    """
    select race_year, team, sum(points) as total_points, 
    sum(
        case
        when position = 1 then 1 else 0 end) 
        as total_wins
    from 
    formula1db.silver.race_results 
    group by race_year, team
    """)

# COMMAND ----------

team_standing_ranked_df = spark.sql(
    """
    select *, dense_rank() over(partition by race_year order by total_points desc, total_wins asc) as team_rank  
    from {table}
    """,
    table = team_standing_df,
)

# COMMAND ----------

# MAGIC %md ##saving to dbfs

# COMMAND ----------

team_standing_ranked_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.silver.team_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formula1db.bronze.circuits

# COMMAND ----------

