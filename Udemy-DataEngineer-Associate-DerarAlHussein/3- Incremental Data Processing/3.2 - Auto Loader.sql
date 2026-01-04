-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Stream Reader (Auto Loader)
-- MAGIC 1. Stream source are parquet files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC  (spark.readStream
-- MAGIC         .format("cloudFiles")
-- MAGIC         .option("cloudFiles.format", "parquet")
-- MAGIC         .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
-- MAGIC         .load(f"{dataset_bookstore}/orders-raw")
-- MAGIC       .writeStream
-- MAGIC         .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
-- MAGIC         .table("orders_updates")
-- MAGIC )

-- COMMAND ----------

SELECT * FROM orders_updates;

SELECT count(*) FROM orders_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Landing New Files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # this is not a Python library function, but defined in ../Includes/Copy-Datasets
-- MAGIC load_new_data()
-- MAGIC
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
-- MAGIC display(files)

-- COMMAND ----------

SELECT count(*) FROM orders_updates

DESCRIBE HISTORY orders_updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Cleaning Up

-- COMMAND ----------

DROP TABLE orders_updates

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
