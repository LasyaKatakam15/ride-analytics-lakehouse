# Databricks notebook source
df = spark.read.format("csv")\
    .option("header",True)\
    .load("/Volumes/pysparkdbt/source/source_data/customers/")

# COMMAND ----------

display(df)

# COMMAND ----------

schema_customers = df.schema
schema_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SPARK STREAMING**

# COMMAND ----------

entities = ["customers", "trips", "locations", "payments", "vehicles", "drivers"]

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, input_file_name

def log(msg):
    print(f"{datetime.datetime.now()} [LOG]: {msg}")

for entity in entities:
    try:
        log(f"Starting ingestion for {entity}")

        path = f"/Volumes/pysparkdbt/source/source_data/{entity}"
        checkpoint = f"/Volumes/pysparkdbt/bronze/checkpoint/{entity}"      
            
        df_batch = spark.read.format("csv")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load(path)

        df = spark.readStream.format("csv")\
            .option("header", True)\
            .option("basePath", path)\
            .schema(df_batch.schema)\
            .load(path)
            
        df = df.withColumn("ingestion_time", current_timestamp()) \
               .withColumn("source_file", input_file_name())

        df.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", checkpoint)\
            .trigger(once=True)\
            .toTable(f"pysparkdbt.bronze.{entity}")

        log(f"Completed ingestion for {entity}")

    except Exception as e:
        log(f"Failed ingestion for {entity}: {e}")
    