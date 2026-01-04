-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed & External Tables in:
-- MAGIC 1. Default database in Hive
-- MAGIC 1. Custom database in Hive 
-- MAGIC 1. Custom database in External 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC remove_directory_if_exists(ext+'/external_default')
-- MAGIC remove_directory_if_exists(ext+'/external_demo')
-- MAGIC remove_directory_if_exists(ext+'/custom.db')
-- MAGIC remove_directory_if_exists(ext+'/external_custom')
-- MAGIC

-- COMMAND ----------

USE DATABASE default; DROP TABLE IF EXISTS external_default;DROP TABLE IF EXISTS managed_default;
DROP DATABASE IF EXISTS demo CASCADE;
DROP DATABASE IF EXISTS custom CASCADE;

-- COMMAND ----------

--managed table where underlying data is stored in csv format (not delta format)
CREATE TABLE IF NOT EXISTS test
(width INT, length INT, height INT)
USING CSV;
DESCRIBE EXTENDED test;
INSERT INTO test VALUES (1,1,1);
DROP TABLE test;


CREATE TABLE IF NOT EXISTS test
(width INT, length INT, height INT) 
LOCATION 'dbfs:/ext/test'
USING CSV;

DESCRIBE EXTENDED test;
DROP TABLE test;


-- COMMAND ----------

describe test;

-- COMMAND ----------

describe extended test;

-- COMMAND ----------

describe detail test;

-- COMMAND ----------

describe history test;

-- COMMAND ----------

USE CATALOG hive_metastore;

CREATE TABLE IF NOT EXISTS managed_default
(width INT, length INT, height INT);

CREATE TABLE IF NOT EXISTS external_default
(width INT, length INT, height INT)
LOCATION 'dbfs:/ext/external_default';

--create external table from a CSV file (data is available) (does not create table directory - no need)
CREATE TABLE IF NOT EXISTS NonDelta_books_csv 
USING CSV
OPTIONS (
  path="${dataset.bookstore}/books-csv"
  ,header = true
  ,delimiter=";"
  );

--create external table with OPTIONS keyword (empty table) (specify where the table directory should be created)
CREATE TABLE IF NOT EXISTS external_default
(width INT, length INT, height INT)
OPTIONS (
  path = 'dbfs:/ext/external_default'
  ,mergeSchema = 'true'
  ,overwriteSchema = 'true'
  );

-------------------------------------------------------------------------------
CREATE DATABASE demo;
USE demo;

CREATE TABLE IF NOT EXISTS managed_demo
(width INT, length INT, height INT);
 
CREATE TABLE IF NOT EXISTS external_demo
(width INT, length INT, height INT)
LOCATION 'dbfs:/ext/external_demo';
----------------------------------------------------------------------------
CREATE DATABASE custom LOCATION 'dbfs:/ext/custom.db';
USE custom;

CREATE TABLE IF NOT EXISTS managed_custom
  (width INT, length INT, height INT);
  
CREATE TABLE IF NOT EXISTS external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/ext/external_custom';
---------------------------------------------------------------
USE CATALOG hive_metastore;
DESCRIBE TABLE EXTENDED managed_default;
DESCRIBE TABLE EXTENDED external_default;
DESCRIBE DATABASE hive_metastore;
DESCRIBE HISTORY employees; --this command only valid for tables
DESCRIBE DETAIL employees; --this command only valid for tables
SHOW TABLES;

USE demo;
DESCRIBE DATABASE EXTENDED demo;
DESCRIBE TABLE EXTENDED managed_demo;
DESCRIBE TABLE EXTENDED external_demo;


USE custom;
DESCRIBE DATABASE EXTENDED custom;
DESCRIBE TABLE EXTENDED managed_custom;
DESCRIBE TABLE EXTENDED external_custom;
---------------------------------------------------------------

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC fs_managed_default = dbutils.fs.ls (hive+'/managed_default')
-- MAGIC display(fs_managed_default)
-- MAGIC
-- MAGIC fs_managed_demo = dbutils.fs.ls (hive+'/demo.db/managed_demo')
-- MAGIC display(fs_managed_demo)
-- MAGIC
-- MAGIC fs_managed_custom = dbutils.fs.ls (ext+'/custom.db/managed_custom')
-- MAGIC display(fs_managed_custom)
-- MAGIC
-- MAGIC fs_external_default = dbutils.fs.ls (ext+'/external_default')
-- MAGIC display(fs_external_default)
-- MAGIC
-- MAGIC fs_external_demo = dbutils.fs.ls (ext+'/external_demo')
-- MAGIC display(fs_external_demo)
-- MAGIC
-- MAGIC fs_external_custom = dbutils.fs.ls (ext+'/external_custom')
-- MAGIC display(fs_external_custom)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Cleanup
USE DATABASE default; 
DROP TABLE external_default;
DROP TABLE managed_default;

DROP DATABASE demo CASCADE;
DROP DATABASE custom CASCADE;
