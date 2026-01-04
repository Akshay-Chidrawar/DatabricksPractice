-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # This Notebook is Used in the Job from Demo 04
-- MAGIC ## Workflow Task 1 - Setup and Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (required)
-- MAGIC To set up your lab environment, execute the following scripts. Be sure to review the environment information provided, and take note of the module's catalog name (**getstarted**) and your unique schema name.

-- COMMAND ----------

-- MAGIC %run ./setup/04_demo_setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configure Your Environment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Set the default catalog to **getstarted** and the schema to your specific schema. Then, view the available tables to confirm that no tables currently exist in your schema.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the catalog and schema
-- MAGIC spark.sql(f'USE CATALOG {learner.catalog_name}')
-- MAGIC spark.sql(f'USE SCHEMA {learner.my_schema}')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### BRONZE
-- MAGIC **Objective:** Create a table using all of the CSV files in the **myfiles** volume.

-- COMMAND ----------

-- Create an empty table and columns
CREATE TABLE IF NOT EXISTS current_employees_bronze (
  ID INT,
  FirstName STRING,
  Country STRING,
  Role STRING,
  InputFile STRING
);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create the bronze raw ingestion table and include the file name for the rows
-- MAGIC copyinto =f"""
-- MAGIC COPY INTO current_employees_bronze
-- MAGIC   FROM (
-- MAGIC       SELECT *, 
-- MAGIC              _metadata.file_name AS InputFile
-- MAGIC       FROM "/Volumes/{learner.catalog_name}/{learner.my_schema}/myfiles/"
-- MAGIC   )
-- MAGIC   FILEFORMAT = CSV
-- MAGIC   FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
-- MAGIC """
-- MAGIC
-- MAGIC
-- MAGIC result = spark.sql(copyinto)
-- MAGIC display(result)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
