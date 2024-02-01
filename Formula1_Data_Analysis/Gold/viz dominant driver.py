# Databricks notebook source
dominant_drivers_overall = spark.sql("select * from formula1db.gold.dominant_drivers_overall")

# COMMAND ----------

df2 = spark.sql(
    """
    select race_year, driver_name, count(1) as total_races, sum(calc_points) as total_calc_points, round(avg(calc_points),2) as avg_points
    from formula1db.silver.calc_race_results
    where driver_name in (select driver_name from {table} where driver_rank <=10 )
    group by race_year, driver_name
    order by race_year, avg_points desc""", table = dominant_drivers_overall
).display()