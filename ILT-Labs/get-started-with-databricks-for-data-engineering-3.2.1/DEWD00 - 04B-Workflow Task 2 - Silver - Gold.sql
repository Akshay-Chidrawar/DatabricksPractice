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
-- MAGIC ## Workflow Task 2 - Silver - Gold

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
-- MAGIC ### SILVER
-- MAGIC **Objective**: Transform the bronze table and insert the resulting rows into the silver table.
-- MAGIC
-- MAGIC 1. Create and display a temporary view named **temp_view_employees_silver** from the **current_employees_bronze** table. 
-- MAGIC
-- MAGIC     The view will:
-- MAGIC     - Select the columns **ID**, **FirstName**, **Country**.
-- MAGIC     - Convert the **Role** column to uppercase.
-- MAGIC     - Add two new columns: **TimeStamp** and **Date**.
-- MAGIC
-- MAGIC     Confirm that the results display 6 rows and 6 columns.

-- COMMAND ----------

-- Create a temporary view to use to merge the data into the final silver table
CREATE OR REPLACE TEMP VIEW temp_view_employees_silver AS 
SELECT 
  ID,
  FirstName,
  Country,
  upper(Role) as Role,                 -- Upcase Role
  current_timestamp() as TimeStamp,    -- Get the current datetime
  date(timestamp) as Date              -- Get the date
FROM current_employees_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Create a new table named **current_employees_silver** and insert rows from the **temp_view_employees_silver** view that are not already present in the table based on the **ID** column.
-- MAGIC
-- MAGIC     Confirm the following:
-- MAGIC     - **num_affected rows** is *6*
-- MAGIC     - **num_updated_rows** is *0*
-- MAGIC     - **num_deleted rows** is *0*
-- MAGIC     - **num_inserted_rows** is *6*

-- COMMAND ----------

-- Create an empty table and specify the column data types
CREATE TABLE IF NOT EXISTS current_employees_silver (
  ID INT,
  FirstName STRING,
  Country STRING,
  Role STRING,
  TimeStamp TIMESTAMP,
  Date DATE
);


-- Insert records from the view when not matched with the target silver table
MERGE INTO current_employees_silver AS target 
  USING temp_view_employees_silver AS source
  ON target.ID = source.ID
  WHEN NOT MATCHED THEN INSERT *            -- Insert rows if no match

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GOLD
-- MAGIC **Objective:** Aggregate the silver table to create the final gold table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create a temporary view named **temp_view_total_roles** that aggregates the total number of employees by role. Then, display the results of the view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW temp_view_total_roles AS 
SELECT
  Role, 
  count(*) as TotalEmployees
FROM current_employees_silver
GROUP BY Role;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Create the final gold table named **total_roles_gold** with the specified columns.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS total_roles_gold (
  Role STRING,
  TotalEmployees INT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Insert all rows from the aggregated temporary view **temp_view_total_rows** into the **total_roles_gold** table, overwriting the existing data in the table.
-- MAGIC
-- MAGIC     Confirm the following:
-- MAGIC     - **num_affected_rows** is *4*
-- MAGIC     - **num_inserted_rows** is *4*

-- COMMAND ----------

INSERT OVERWRITE TABLE total_roles_gold
SELECT * 
FROM temp_view_total_roles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
