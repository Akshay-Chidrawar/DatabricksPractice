# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

# .readStream returns a "DataStreamReader" object.This object is used to read streaming data as a DataFrame. createOrReplaceTempView registers a temp view on a dataframe, and does not return anything. Thus, do not assign its results to a variable. createOrReplaceTempView is intended to use only on static dataframes, but can be used for streaming dataframes as well.

rs_df = spark.readStream.table(tbl1)
print(type(rs_df))
display(rs_df)
rs_df.createOrReplaceTempView('bronze_tmp')
display(spark.sql(f"select * from bronze_tmp"))

# COMMAND ----------

spark.streams.active
# stream is created only when you run query SELECT * FROM stream_source. No streams created for read stream or write stream objects. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##SQL view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS  
# MAGIC SELECT v.* FROM  
# MAGIC (SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING,   quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v  
# MAGIC FROM bronze_tmp  
# MAGIC WHERE topic = "orders")
# MAGIC
# MAGIC ##Notes
# MAGIC from_json returns "struct object", thus alias "v" is not on single column, but the entire object. When we use v.* in parent SELECT statement, all columns are read in.
# MAGIC
# MAGIC ##Python stream
# MAGIC query = (spark.table("orders_silver_tmp")  
# MAGIC                .writeStream  
# MAGIC                .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")  
# MAGIC                .trigger(availableNow=True)  
# MAGIC                .table("orders_silver"))  
# MAGIC
# MAGIC query.awaitTermination()
# MAGIC

# COMMAND ----------

process_silver_orders()
display(spark.sql(f"select * from {tbl4}"))
