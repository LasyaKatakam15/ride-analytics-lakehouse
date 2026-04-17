# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import List
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import datetime 

# COMMAND ----------

def log(msg):
    print(f"{datetime.datetime.now()} [LOG]: {msg}")

class transformations:
    def dedup(self, df:DataFrame, dedup_cols: List,cdc:str):
        try:
            log("Running deduplication...") 

            df = df.withColumn("dedupKey",concat(*dedup_cols))
            df = df.withColumn("dedupCounts", row_number()
                            .over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
            df = df.filter(col("dedupCounts")==1)
            df = df.drop("dedupKey", "dedupCounts")
            return df
        
        except Exception as e:
            log(f"Dedup failed: {e}")   
            raise

    def process_timestamp(self, df):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def upsert(self, df,key_cols, table,cdc):
        try:
            log(f"Upserting into {table}")

            merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])

            dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")

            target_cols = dlt_obj.toDF().columns
            source_cols = df.columns

            common_cols = [c for c in source_cols if c in target_cols]

            update_dict = {c: f"src.{c}" for c in common_cols}
            insert_dict = {c: f"src.{c}" for c in common_cols}

            dlt_obj.alias("trg").merge(df.alias("src"), merge_condition)\
                .whenMatchedUpdate(
                    condition=f"src.{cdc} >= trg.{cdc}",
                    set=update_dict
                )\
                .whenNotMatchedInsert(
                    values=insert_dict
                )\
                .execute()

            log(f"Upsert completed for {table}")
            return 1

        except Exception as e:
            log(f"Upsert failed for {table}: {e}")   
            raise


# COMMAND ----------

import os
import sys
current_dir = os.getcwd()
sys.path.append(current_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Customers**

# COMMAND ----------

log("Processing customers") 
df_cust = spark.read.table("pysparkdbt.bronze.customers")
df_cust = df_cust.repartition(4)

# COMMAND ----------

expected_cols = ["customer_id","email","first_name","last_name","phone_number","last_updated_timestamp"]

missing_cols = [c for c in expected_cols if c not in df_cust.columns]

if missing_cols:
    raise Exception(f"Missing columns in customers: {missing_cols}")

extra_cols = [c for c in df_cust.columns if c not in expected_cols]

if extra_cols:
    log(f"Extra columns found in customers: {extra_cols}")

if df_cust.filter(col("customer_id").isNull()).count() > 0:
    raise Exception("Null customer_id found")

dup_count = df_cust.groupBy("customer_id").count().filter("count > 1").count()
if dup_count > 0:
    log(f"Found {dup_count} duplicate customer_id records")

if df_cust.filter(length(col("phone_number")) < 10).count() > 0:
    log("Invalid phone numbers found")

# COMMAND ----------

df_cust = df_cust.select("customer_id","email","first_name","last_name","phone_number","last_updated_timestamp")

df_cust = df_cust.withColumn("domain",split(col("email"), "@")[1])
df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]",""))
df_cust = df_cust.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
df_cust = df_cust.drop("first_name", "last_name")



# COMMAND ----------

cust_obj = transformations()
cust_of_trans = cust_obj.dedup(df_cust,['customer_id'],'last_updated_timestamp')
df_cust = cust_obj.process_timestamp(cust_of_trans)


# COMMAND ----------

if df_cust.count() == 0:
    raise Exception("No data to write for customers")

if not spark.catalog.tableExists( "pysparkdbt.silver.customers"):

    df_cust.write.format("delta")\
             .mode("append")\
            .saveAsTable("pysparkdbt.silver.customers")
else:
    cust_obj.upsert(df_cust, ['customer_id'], 'customers', 'last_updated_timestamp')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pysparkdbt.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DRIVERS**

# COMMAND ----------

log("Processing drivers") 
df_drivers = spark.read.table("pysparkdbt.bronze.drivers")
df_drivers = df_drivers.repartition(4)

# COMMAND ----------

expected_cols = ["driver_id","first_name","last_name","phone_number","last_updated_timestamp"]

missing_cols = [c for c in expected_cols if c not in df_drivers.columns]

if missing_cols:
    raise Exception(f"Missing columns in drivers: {missing_cols}")

extra_cols = [c for c in df_drivers.columns if c not in expected_cols]

if extra_cols:
    log(f"Extra columns found in drivers: {extra_cols}")


if df_drivers.filter(col("driver_id").isNull()).count() > 0:
    raise Exception("Null driver_id found")

dup_count = df_drivers.groupBy("driver_id").count().filter("count > 1").count()
if dup_count > 0:
    log(f"Found {dup_count} duplicate driver_id")

if df_drivers.filter(length(col("phone_number")) < 10).count() > 0:
    log("Invalid driver phone numbers")

# COMMAND ----------

df_drivers = df_drivers.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]",""))
df_drivers = df_drivers.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
df_drivers = df_drivers.drop("first_name", "last_name")

# COMMAND ----------

driver_obj = transformations()
df_driver = driver_obj.dedup(df_drivers, ['driver_id'], 'last_updated_timestamp')
df_drivers = driver_obj.process_timestamp(df_driver)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):
    df_drivers.write.format("delta")\
                    .mode("append")\
                    .saveAsTable("pysparkdbt.silver.drivers")
else:
    driver_obj.upsert(df_drivers, ['driver_id'], 'drivers', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Locations**

# COMMAND ----------

log("Processing locations") 
df_loc =  spark.read.table("pysparkdbt.bronze.locations")
df_loc = df_loc.repartition(4) 

# COMMAND ----------

expected_cols = ["location_id","city","state","last_updated_timestamp"]

missing_cols = [c for c in expected_cols if c not in df_loc.columns]
if missing_cols:
    raise Exception(f"Missing columns in locations: {missing_cols}")
extra_cols = [c for c in df_loc.columns if c not in expected_cols]

if df_loc.filter(col("location_id").isNull()).count() > 0:
    raise Exception("Null location_id found")

dup_count = df_loc.groupBy("location_id").count().filter("count > 1").count()
if dup_count > 0:
    log(f"Found {dup_count} duplicate location_id")

# COMMAND ----------

loc_obj = transformations()
df_loc = loc_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
df_loc = loc_obj.process_timestamp(df_loc)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):
    df_loc.write.format("delta")\
                .mode("append")\
                .saveAsTable("pysparkdbt.silver.locations")
else:
    loc_obj.upsert(df_loc, ['location_id'], 'locations', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pysparkdbt.silver.locations

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Payments**

# COMMAND ----------

log("Processing payments") 
df_pay = spark.read.table("pysparkdbt.bronze.payments")
df_pay = df_pay.repartition(4) 

# COMMAND ----------

df_pay = df_pay.withColumn("payment_status", lower(col("payment_status")))

valid_status = ["success","failed","pending"]
invalid_count = df_pay.filter(~col("payment_status").isin(valid_status)).count()

if invalid_count > 0:
    log(f"Found {invalid_count} invalid payment_status records")

df_pay = df_pay.filter(col("payment_status").isin(valid_status))

if df_pay.filter(col("payment_id").isNull()).count() > 0:
    raise Exception("Null payment_id found")

dup_count = df_pay.groupBy("payment_id").count().filter("count > 1").count()
if dup_count > 0:
    log(f"Found {dup_count} duplicate payment_id")

# COMMAND ----------

df_pay = df_pay.withColumn("online_payment_status",
     when( ((col('payment_method')== 'Card') & (col('payment_status')=='Success')), 'online_success')
    .when( ((col('payment_method')== 'Card') & (col('payment_status')=='Failed')), 'online_failed')
    .when( ((col('payment_method')== 'Card') & (col('payment_status')=='pending')), 'online_pending')
    .otherwise("offline")
    )
display(df_pay)

# COMMAND ----------

payment_obj = transformations()
df_pay = payment_obj.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
df_pay = payment_obj.process_timestamp(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists('pysparkdbt.silver.payments'):
    df_pay.write.format("delta")\
                .mode("append")\
                .saveAsTable("pysparkdbt.silver.payments")
else:
    payment_obj.upsert(df_pay, ['payment_id'], 'payments', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pysparkdbt.silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Vehicles**

# COMMAND ----------

log("Processing vehicles")
df_veh = spark.read.table("pysparkdbt.bronze.vehicles")
df_veh = df_veh.repartition(4) 

# COMMAND ----------

expected_cols = ["vehicle_id","make","model","year","last_updated_timestamp"]
missing_cols = [c for c in expected_cols if c not in df_veh.columns]
if missing_cols:
    raise Exception(f"Missing columns in vehicles: {missing_cols}")

extra_cols = [c for c in df_veh.columns if c not in expected_cols]

if extra_cols:
    log(f"Extra columns found in vehicles: {extra_cols}")

if df_veh.filter(col("vehicle_id").isNull()).count() > 0:
    raise Exception("Null vehicle_id found")

dup_count = df_veh.groupBy("vehicle_id").count().filter("count > 1").count()
if dup_count > 0:
    log(f"Found {dup_count} duplicate vehicle_id")

# COMMAND ----------

print(df_veh.columns)

# COMMAND ----------

df_veh = df_veh.withColumn("make", upper(col('make')))


# COMMAND ----------

vehicle_obj = transformations()
df_veh = vehicle_obj.dedup(df_veh,['vehicle_id'], 'last_updated_timestamp')
df_veh =vehicle_obj.process_timestamp(df_veh)

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):
    df_veh.write.format("delta")\
                .mode("append")\
                .saveAsTable("pysparkdbt.silver.vehicles")
else:
    vehicle_obj.upsert(df_veh, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pysparkdbt.silver.vehicles

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC