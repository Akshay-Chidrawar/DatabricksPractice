-- Databricks notebook source
USE CATALOG hive_metastore;
SHOW TABLES;
SHOW TABLES IN global_temp;
SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;
DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;
