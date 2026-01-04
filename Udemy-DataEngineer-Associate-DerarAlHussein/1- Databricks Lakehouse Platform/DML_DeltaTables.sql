-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create new Database & new Delta Table & check in the Catalog

-- COMMAND ----------

-- DBTITLE 1,Create Delta Table & check in the Catalog
DROP DATABASE IF EXISTS demo cascade;

CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;
CREATE TABLE employees
(
id INT, 
name STRING,
salary DOUBLE
);

-- COMMAND ----------

-- DBTITLE 1,Insert Data

INSERT INTO employees VALUES 
(1, "Adam", 3500.0),
(2, "Sarah", 4020.5);

INSERT INTO employees VALUES
(3, "John", 2999.3),
(4, "Thomas", 4000.3);

INSERT INTO employees VALUES
(5, "Anna", 2500.0);

INSERT INTO employees VALUES
(6, "Kim", 6200.3)


-- COMMAND ----------

-- DBTITLE 1,Explore Table Directory
-- MAGIC %python
-- MAGIC file_path = 'dbfs:/user/hive/warehouse/demo.db/employees'
-- MAGIC filepath_contents = dbutils.fs.ls (file_path)
-- MAGIC parquet_files = [file for file in filepath_contents if file.name.endswith(".parquet")]
-- MAGIC json_files = [file for file in dbutils.fs.ls (file_path+'/_delta_log/') if file.name.endswith(".json")]
-- MAGIC display (parquet_files)
-- MAGIC display (json_files)

-- COMMAND ----------

-- DBTITLE 1,Query table
SELECT * FROM employees;
DESCRIBE DETAIL employees; --this command only valid for tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Update Table
-- MAGIC 1. Update the table > Check detail > Check files list > Compare with earlier version. 
-- MAGIC 2. Check history 

-- COMMAND ----------

-- DBTITLE 1,Update Table
UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

-- DBTITLE 1,Explore Table Directory
-- MAGIC %python
-- MAGIC filepath_contents = dbutils.fs.ls (file_path)
-- MAGIC parquet_files = [file for file in filepath_contents if file.name.endswith(".parquet")]
-- MAGIC json_files = [file for file in dbutils.fs.ls (file_path+'/_delta_log/') if file.name.endswith(".json")]
-- MAGIC display (parquet_files)
-- MAGIC display (json_files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head('dbfs:/user/hive/warehouse/demo.db/employees/_delta_log/00000000000000000005.json')

-- COMMAND ----------

DROP DATABASE demo cascade;
