# Databricks notebook source
dominant_team_overall = spark.sql("select * from formula1db.gold.dominant_teams_overall")

# COMMAND ----------

df2 = spark.sql(
    """
    select race_year, team, count(1) as total_races, sum(calc_points) as total_calc_points, round(avg(calc_points),2) as avg_points
    from formula1db.silver.calc_race_results
    where team in (select team from {table} where team_rank <=10 )
    group by race_year, team
    order by race_year, avg_points desc""", table = dominant_team_overall
).display()

# COMMAND ----------

