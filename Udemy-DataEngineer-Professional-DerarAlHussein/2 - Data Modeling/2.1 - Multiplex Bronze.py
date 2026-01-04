# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

cleanup_and_setup()

# COMMAND ----------

files = dbutils.fs.ls(kafka_raw)
display(files)

df_raw = spark.read.format("json").load(kafka_raw) 
df_raw.createOrReplaceTempView('vw_raw')
display(spark.sql("select key,value,topic,partition,offset,timestamp from vw_raw order by partition,offset"))

# COMMAND ----------

process_bronze()
display(spark.sql(f"select * from {tbl1}"))

load_new_data()
process_bronze()
display(spark.sql(f"select * from {tbl1}"))

# COMMAND ----------

spark.streams.active
