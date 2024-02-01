# Databricks notebook source
client_id = "e166af46-83b7-4da2-b3ef-52908e805f39"
tenant_id = "f3ee9e6f-137c-4e25-9826-b7ad6a80b3e9"
secret = "Rq68Q~LKuNxjiiv3C-YDCGBvXS5c1~~rdo.sNcV3"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": client_id,
"fs.azure.account.oauth2.client.secret": secret,
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
source = "abfss://reservoir@avkrgadls.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/avkrgadls",
extra_configs = configs)

# COMMAND ----------

# athletes = spark.read.option("header", True).csv("/mnt/avkrgadls/OlympicDA/raw-data/athletes.csv")
# coaches = spark.read.option("header", True).csv("/mnt/avkrgadls/OlympicDA/raw-data/coaches.csv")
# entriesGender = spark.read.option("header", True).csv("/mnt/avkrgadls/OlympicDA/raw-data/entriesGender.csv")
# medals = spark.read.option("header", True).csv("/mnt/avkrgadls/OlympicDA/raw-data/medals.csv")
# teams = spark.read.option("header", True).csv("/mnt/avkrgadls/OlympicDA/raw-data/teams.csv")

# COMMAND ----------

# MAGIC %md ###creating database inside catalog

# COMMAND ----------

# spark.sql("CREATE SCHEMA IF NOT EXISTS formula1db.bronze")
# spark.sql("CREATE SCHEMA IF NOT EXISTS formula1db.silver")
# spark.sql("CREATE SCHEMA IF NOT EXISTS formula1db.gold")