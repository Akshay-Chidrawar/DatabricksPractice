# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

cleanup_and_setup()

# COMMAND ----------

# DBTITLE 1,Streams
from pyspark.sql import functions as F

batch_orders_dedup = (
spark.read.table(tbl1)
.filter("topic = 'orders'")
.select(F.from_json(F.col("value").cast("string"), tbl4Schema).alias("v"))
.select("v.*")
.dropDuplicates(["order_id", "order_timestamp"])
)

process_silver_orders()

batch_orders_dedup_count = batch_orders_dedup.count()
stream_orders_dedup_count = spark.read.table(tbl4).count()

assert batch_orders_dedup_count == stream_orders_dedup_count
print('Batch and streaming counts match',batch_orders_dedup_count,stream_orders_dedup_count)
