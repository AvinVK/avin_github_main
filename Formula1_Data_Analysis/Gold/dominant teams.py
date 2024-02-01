# Databricks notebook source
# MAGIC %md ##teams dominated overall

# COMMAND ----------

dominant_team_df = spark.sql(
    """
    select team, count(1) as total_races, sum(calc_points) as total_calc_points, round(avg(calc_points),2) as avg_points, rank() over(order by round(avg(calc_points),2) desc) as team_rank
    from formula1db.silver.calc_race_results
    group by team 
    having total_races >=100 """
)

# COMMAND ----------

# MAGIC %md ##teams dominated in last decade

# COMMAND ----------

dominant_team_last_decade = spark.sql(
    """
    select team, count(1) as total_races, sum(calc_points) as total_calc_points, round(avg(calc_points),2) as avg_points
    from formula1db.silver.calc_race_results
    where race_year >= 2010
    group by team 
    having total_races >=100 """
)

# COMMAND ----------

dominant_team_df.display()

# COMMAND ----------

dominant_team_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("formula1db.gold.dominant_teams_overall")

# COMMAND ----------

