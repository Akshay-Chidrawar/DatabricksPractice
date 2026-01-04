-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Get Started with Databricks for Data Engineering
-- MAGIC ## Demo 03 - Transforming Data Using the Medallion Architecture
-- MAGIC
-- MAGIC <br></br>
-- MAGIC ![medallion_architecture](files/images/get-started-with-databricks-for-data-engineering-3.2.1/medallion_architecture.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (required)
-- MAGIC To set up your lab environment, execute the following scripts. Be sure to review the environment information provided, and take note of the module's catalog name (**getstarted**) and your unique schema name.

-- COMMAND ----------

-- MAGIC %run ./setup/03_demo_setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Input widgets enable you to add parameters to your notebooks. After creating these widgets, you can view the text values for the parameters **module_catalog** and **my_schema** at the top of the notebook. These values specify the module's catalog name and your specific schema.
-- MAGIC
-- MAGIC **NOTE**: Leaving the notebook can cause the widget values to be cleared. You will have to rerun the setup script and code below to reset the values. If you modify the widget values, please manually delete them and rerun the setup again.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Set widget values using the setup script
-- MAGIC dbutils.widgets.text("module_catalog",learner.catalog_name)
-- MAGIC dbutils.widgets.text("my_schema", learner.my_schema)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## a. Configure and Explore Your Environment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Set the default catalog to **getstarted** and the schema to your specific schema. Then, view the available tables to confirm that no tables currently exist in your schema.

-- COMMAND ----------

-- Set the catalog and schema
USE CATALOG ${module_catalog};
USE SCHEMA IDENTIFIER(:my_schema);

-- Display available tables in your schema
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View the available files in your schema's **myfiles** volume. Confirm that the volume contains two CSV files, **employees.csv** and **employees2.csv**.

-- COMMAND ----------

LIST '/Volumes/${module_catalog}/${my_schema}/myfiles/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## b. Simple Example of the Medallion Architecture
-- MAGIC
-- MAGIC **Objective**: Create a pipeline that can be scheduled to run automatically. The pipeline will:
-- MAGIC
-- MAGIC 1. Ingest all CSV files from the **myfiles** volume and create a bronze table.
-- MAGIC 2. Prepare the bronze table by adding new columns and create a silver table.
-- MAGIC 3. Create a gold aggregated table for consumers.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### BRONZE
-- MAGIC **Objective:** Create a table using all of the CSV files in the **myfiles** volume.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute the cell to perform the following:
-- MAGIC     - The DROP TABLE IF EXISTS statement drops the **current_employees_bronze** table if it already exists for demonstration purposes.
-- MAGIC     - The CREATE TABLE IF NOT EXISTS statement creates the Delta table **current_employees_bronze** if it doesn't already exist and defines the table columns.
-- MAGIC     - The COPY INTO statement:
-- MAGIC         - loads all the CSV files from the **myfiles** volume in your schema into the **current_employees_bronze** table and creates a new column named **InputFile** that displays the file from where the data came. 
-- MAGIC         - It uses the first row as headers and infers the schema from the CSV files.
-- MAGIC     - The query will display all rows from the **current_employees_bronze** table.
-- MAGIC
-- MAGIC     Confirm that the table has 6 rows and 5 columns.
-- MAGIC
-- MAGIC     **NOTE:** The  [input_file_name](https://docs.databricks.com/en/sql/language-manual/functions/input_file_name.html) function is not available on Unity Catalog. You can get metadata information for input files with the [_metadata](https://docs.databricks.com/en/ingestion/file-metadata-column.html) column.

-- COMMAND ----------

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS current_employees_bronze;


-- Create an empty table and columns
CREATE TABLE IF NOT EXISTS current_employees_bronze (
  ID INT,
  FirstName STRING,
  Country STRING,
  Role STRING,
  InputFile STRING
);


-- Create the bronze raw ingestion table and include the CSV file name for the rows
COPY INTO current_employees_bronze
  FROM (
      SELECT *, 
             _metadata.file_name AS InputFile               -- Add the input file name to the bronze table
      FROM '/Volumes/${module_catalog}/${my_schema}/myfiles/'
  )
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');


-- View the bronze table
SELECT * 
FROM current_employees_bronze;

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
  upper(Role) as Role,                 -- Upcase the Role column
  current_timestamp() as TimeStamp,    -- Get the current datetime
  date(timestamp) as Date              -- Get the date
FROM current_employees_bronze;


-- Display the results of the view
SELECT * 
FROM temp_view_employees_silver;

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

-- Dropping the table for demonstration purposes
DROP TABLE IF EXISTS current_employees_silver;


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
-- MAGIC 3. View the **current_employees_silver** table. Confirm the table contains all 6 employees.

-- COMMAND ----------

SELECT * 
FROM current_employees_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Run the same MERGE INTO statement again. Note that since all the **ID** values matched, no rows were inserted into the **current_employees_silver**  table.

-- COMMAND ----------

MERGE INTO current_employees_silver AS target 
  USING temp_view_employees_silver AS source
  ON target.ID = source.ID
  WHEN NOT MATCHED THEN INSERT *

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


SELECT *
FROM temp_view_total_roles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Create the final gold table named **total_roles_gold** with the specified columns.

-- COMMAND ----------

-- Dropping the table for demonstration purposes
DROP TABLE IF EXISTS total_roles_gold;

CREATE TABLE IF NOT EXISTS total_roles_gold (
  Role STRING,
  TotalEmployees INT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Insert all rows from the aggregated temporary view **temp_view_total_rows** into the **total_roles_gold** table, overwriting the existing data in the table. This will allow you to see the history as the gold table is updated.
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
-- MAGIC 4. Query the **total_roles_gold** table to view the total number of employees by role.

-- COMMAND ----------

SELECT *
FROM total_roles_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## c. Data Governance and Security
-- MAGIC **Objectives:** View the lineage of the **total_roles_gold** table and learn how to set its permissions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Complete the following to open your schema in the **Catalog Explorer**.
-- MAGIC - a. Select the Catalog icon ![catalog_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/catalog_icon.png) in the left navigation bar. 
-- MAGIC - b. Type the module's catalog name in the search bar (*getstarted*).
-- MAGIC - c. Select the refresh icon ![refresh_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/refresh_icon.png) to refresh the **getstarted** catalog.
-- MAGIC - d. Expand the **getstarted** catalog. Within the catalog, you should see a variety of schemas (databases).
-- MAGIC - e. Find and select the your schema. You can locate your schema in the setup notes in the first cell or in the top widget bar under the **my_schema** parameter. 
-- MAGIC - f.  Click the options ![catalog_options](files/images/get-started-with-databricks-for-data-engineering-3.2.1/catalog_options.png) icon to the right of your schema and choose **Open in Catalog Explorer**.
-- MAGIC - g. Notice that the three tables we created in the demo: **current_employees_bronze**, **current_employees_silver** and **total_roles_gold** are shown in the **Catalog Explorer** for your schema.
-- MAGIC - h. In the **Catalog Explorer** select the **total_roles_gold** table.
-- MAGIC
-- MAGIC Leave the **Catalog Explorer** tab open.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Complete the following to view the **total_roles_gold** table's permissions, history, lineage and insights in Catalog Explorer: 
-- MAGIC - a. **Permissions**. 
-- MAGIC   - Select the **Permissions** tab. This will display all permissions on the table. Currently the table does not have any permissions set.
-- MAGIC   - Select **Grant**. This allows you to add multiple principals and assign privileges to them. Users must have access to the Catalog and Schema of the table.
-- MAGIC   - Select **Cancel**. 
-- MAGIC - b. **History**
-- MAGIC   - Select the **History** tab. This will display the table's history. The **total_roles_gold** table currently has two versions.
-- MAGIC - c. **Lineage**
-- MAGIC   - Select the **Lineage** tab. This displays the table's lineage. Confirm that the **current_employees_silver** table is shown.
-- MAGIC   - Select the **See lineage graph** button. This displays the table's lineage visually. You can select the **+** icon to view additional information.
-- MAGIC   - Close out of the lineage graph.
-- MAGIC - d. **Insights**
-- MAGIC   - Select the **Insights** tab. You can use the Insights tab in **Catalog Explorer** to view the most frequent recent queries and users of any table registered in Unity Catalog. The Insights tab reports on frequent queries and user access for the past 30 days.
-- MAGIC - e. Close the **Catalog Explorer** browser tab.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##d. Cleanup
-- MAGIC 1. Drop views and tables.

-- COMMAND ----------

-- Drop the temporary views
DROP VIEW IF EXISTS temp_view_total_roles;
DROP VIEW IF EXISTS temp_view_employees_silver;

-- Drop the tables
DROP TABLE IF EXISTS current_employees_bronze;
DROP TABLE IF EXISTS current_employees_silver;
DROP TABLE IF EXISTS total_roles_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
