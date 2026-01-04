-- Databricks notebook source
create database akshay

-- COMMAND ----------

drop database akshay;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/akshay.db')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC str1='hello'
-- MAGIC str2='world'
-- MAGIC tbl_name=str1+str2
-- MAGIC print (str1+str2)
-- MAGIC
-- MAGIC create_table_query = f"""
-- MAGIC CREATE TABLE {tbl_name} (
-- MAGIC     id INT
-- MAGIC )
-- MAGIC """
-- MAGIC
-- MAGIC # Execute the SQL query
-- MAGIC spark.sql(create_table_query)

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## File formats having well defined schema (JSON)

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`;
SELECT *,input_file_name() as source_file FROM json.`${dataset.bookstore}/customers-json/export_*.json`;
SELECT *,input_file_name() as source_file FROM json.`${dataset.bookstore}/customers-json`;
SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`;

SELECT * FROM text.`${dataset.bookstore}/customers-json/export_001.json`;
SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json/export_001.json`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC json_files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(json_files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##File formats not having well defined schema (Eg- CSV)
-- MAGIC 1. Create Non Delta table (external) from csv data. Use USING keyword.
-- MAGIC 2. Create Delta table (managed). First create a TEMP VIEW from csv data (use USING keyword).
-- MAGIC 3. If source csv file gets modified, non delta table will not refresh on its own. Manual refresh needs to be triggered in order to reflect latest data. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC csv_files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(csv_files)

-- COMMAND ----------

USE default;
DROP TABLE IF EXISTS NonDelta_books_csv;
CREATE TABLE NonDelta_books_csv 
USING CSV
OPTIONS (path="${dataset.bookstore}/books-csv",header = true, delimiter=";");

SELECT * FROM NonDelta_books_csv;
DESCRIBE EXTENDED NonDelta_books_csv;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_books_csv 
USING CSV
OPTIONS (path="${dataset.bookstore}/books-csv",header = true, delimiter=";");

USE default;
DROP TABLE IF EXISTS Delta_books_csv;
CREATE TABLE Delta_books_csv AS
SElECT * FROM vw_books_csv;

SElECT * FROM Delta_books_csv;
DESCRIBE EXTENDED Delta_books_csv;

-- COMMAND ----------

SELECT COUNT(*) FROM NonDelta_books_csv

-- COMMAND ----------

REFRESH TABLE NonDelta_books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM NonDelta_books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Use CTAS statement to create delta tables from json/ binary/ csv.

-- COMMAND ----------

USE default;
DROP TABLE IF EXISTS CTAS_json;
DROP TABLE IF EXISTS CTAS_binary;
DROP TABLE IF EXISTS CTAS_csv;

--json format
CREATE TABLE CTAS_json AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json`;
--binaryFile format
CREATE TABLE CTAS_binary AS
SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`;
--csv format
CREATE TABLE CTAS_csv AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM CTAS_json;
SELECT * FROM CTAS_binary;
SELECT * FROM CTAS_csv;

DESCRIBE EXTENDED CTAS_json;
DESCRIBE EXTENDED CTAS_binary;
DESCRIBE EXTENDED CTAS_csv;

-- COMMAND ----------

DROP TABLE IF EXISTS NonDelta_books_csv;
DROP TABLE IF EXISTS Delta_books_csv;

DROP TABLE IF EXISTS CTAS_json;
DROP TABLE IF EXISTS CTAS_binary;
DROP TABLE IF EXISTS CTAS_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #EXIT

-- COMMAND ----------

-- DBTITLE 1,EXIT
-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Notebook execution completed.")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import re
-- MAGIC # Define the directory and regex pattern
-- MAGIC directory = 'dbfs:/mnt/demo-datasets/bookstore/books-csv'
-- MAGIC pattern = re.compile(r'export_')
-- MAGIC
-- MAGIC # List all files in the directory
-- MAGIC files = dbutils.fs.ls(directory)
-- MAGIC
-- MAGIC # Filter files that do not match the regex pattern
-- MAGIC non_matching_files = [file.path for file in files if not pattern.match(file.name)]
-- MAGIC
-- MAGIC # Remove non-matching files
-- MAGIC for file_path in non_matching_files:
-- MAGIC     dbutils.fs.rm(file_path)
-- MAGIC
-- MAGIC dbutils.fs.rm('dbfs:/mnt/demo-datasets/bookstore/books-csv/export_005.csv')
-- MAGIC dbutils.fs.rm('dbfs:/mnt/demo-datasets/bookstore/books-csv/export_006.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def import_new_files(source,target):
-- MAGIC     csv_files_new = dbutils.fs.ls(source)
-- MAGIC     for f in csv_files_new:
-- MAGIC         dbutils.fs.cp(f"{source}/{f.name}",f"{target}/{f.name}")
-- MAGIC
-- MAGIC def remove_new_files(source,target):
-- MAGIC     csv_files_new_source = dbutils.fs.ls(source)
-- MAGIC     csv_files_new_target = dbutils.fs.ls(target)
-- MAGIC     csv_files_new_source_names = {file.name for file in csv_files_new_source}
-- MAGIC     csv_files_new_target_names = {file.name for file in csv_files_new_target}
-- MAGIC     csv_files_new_common_names = csv_files_new_source_names.intersection(csv_files_new_target_names)    
-- MAGIC     for file_name in csv_files_new_common_names:
-- MAGIC         dbutils.fs.rm(f"{target}/{file_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC source = f"{dataset_bookstore}/books-csv-new"
-- MAGIC target = f"{dataset_bookstore}/books-csv"
-- MAGIC print(source)
-- MAGIC
-- MAGIC #import_new_files(source,target)
-- MAGIC #remove_new_files(source,target)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #If you have not dropped the delta tables while the cluster is active, you will face errors while recreating those tables after the cluster restarts. Reason: cluster centric metastore & conflict with physical location.
-- MAGIC #dbutils.fs.rm('dbfs:/user/hive/warehouse/delta_books_csv',recurse=True)
-- MAGIC #dbutils.fs.rm('dbfs:/user/hive/warehouse/ctas_json',recurse=True)
-- MAGIC #dbutils.fs.rm('dbfs:/user/hive/warehouse/ctas_binary',recurse=True)
-- MAGIC #dbutils.fs.rm('dbfs:/user/hive/warehouse/ctas_csv',recurse=True)

-- COMMAND ----------

-- DBTITLE 1,Add some more csv files to books_csv directory
-- MAGIC %python
-- MAGIC (
-- MAGIC   spark.read
-- MAGIC         .table("NonDelta_books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv")
-- MAGIC )
