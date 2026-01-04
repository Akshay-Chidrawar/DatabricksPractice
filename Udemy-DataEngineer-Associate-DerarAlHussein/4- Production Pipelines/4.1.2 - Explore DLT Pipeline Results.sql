-- Databricks notebook source
--results of "gold" LIVE tables created in DLT pipeline are stored in below "target database" which we specified during DLT pipeline creation 
SELECT * FROM hive_metastore.demo_bookstore_dlt_db.cn_daily_customer_books;
SELECT * FROM hive_metastore.demo_bookstore_dlt_db.fr_daily_customer_books;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #DLT pipeline logs, DLT tables, etc. are stored in below storage path which we specified during DLT pipeline creation.
-- MAGIC DLT_contents = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore")
-- MAGIC display(DLT_contents)
-- MAGIC
-- MAGIC event_files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/system/events")
-- MAGIC display(event_files)
-- MAGIC
-- MAGIC DLT_tables = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/tables")
-- MAGIC display(DLT_tables)

-- COMMAND ----------

--event logs stored as a delta table
SELECT * FROM delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`
