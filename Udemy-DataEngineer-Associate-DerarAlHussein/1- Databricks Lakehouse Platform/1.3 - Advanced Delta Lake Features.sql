-- Databricks notebook source
CREATE MATERIALIZED VIEW MV_TMP AS
SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create new table > Perform CRUD operations > Check history > Access particular version.
-- MAGIC 2. Optimize table > Check detail > Check history > Restore earlier version. 
-- MAGIC 3. Check current retention period > Modify it to 0 & 1 hour > Check files list > From history, try accessing some version which is impacted due to retention period got modified. 

-- COMMAND ----------

USE DATABASE demo;
DESCRIBE HISTORY employees; --this command only valid for tables
DESCRIBE DETAIL employees; --this command only valid for tables
---------------------------------------------------------
SELECT * FROM employees TIMESTAMP AS OF '2024-12-03T13:25:25.000+00:00';
SELECT * FROM employees VERSION AS OF 1;
SELECT * FROM employees@v2;
---------------------------------------------------------
optimize employees
zorder by (id);
DESCRIBE DETAIL employees;
------------------------------------------------------------------------------------
RESTORE TABLE employees TO VERSION AS OF 5;
RESTORE TABLE employees TO TIMESTAMP AS OF '2024-12-03T13:25:25.000+00:00';
-------------------------------------------------------------------------------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_path = 'dbfs:/user/hive/warehouse/demo.db/employees'
-- MAGIC parquet_files = [file for file in dbutils.fs.ls (file_path) if file.name.endswith(".parquet")]
-- MAGIC json_files = [file for file in dbutils.fs.ls (file_path+'/_delta_log/') if file.name.endswith(".json")]
-- MAGIC display (parquet_files)
-- MAGIC display (json_files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

-- COMMAND ----------

-- DBTITLE 1,Vacuum
VACUUM employees RETAIN 0 HOURS;

-- COMMAND ----------

select * from employees VERSION AS OF 6
