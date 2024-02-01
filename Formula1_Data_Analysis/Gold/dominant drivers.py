# Databricks notebook source
# MAGIC %md ##drivers dominated overall

# COMMAND ----------

dominant_drivers_df = spark.sql(
    """
    select driver_name, count(1) as total_races, sum(calc_points) as total_calc_points, round(avg(calc_points),2) as avg_points, rank() over(order by round(avg(calc_points),2) desc) as driver_rank
    from formula1db.silver.calc_race_results
    group by driver_name 
    having total_races >=50 """
)

# COMMAND ----------

dominant_drivers_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.gold.dominant_drivers_overall")

# COMMAND ----------

# MAGIC %md ##drivers dominated in last decade

# COMMAND ----------

df2 = spark.sql(
    """
    select driver_name, count(1) as total_races, sum(calc_points) as total_calc_points, round(avg(calc_points),2) as avg_points
    from formula1db.silver.calc_race_results
    where race_year >= 2010
    group by driver_name 
    having total_races >=50 """
)

# COMMAND ----------

