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
-- MAGIC ## Demo 02 - Ingesting Data into Delta Lake

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (required)
-- MAGIC To set up your lab environment, execute the following scripts. Be sure to review the environment information provided, and take note of the module's catalog name (**getstarted**) and your unique schema name.

-- COMMAND ----------

-- MAGIC %run ./setup/02_demo_setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Input widgets enable you to add parameters to your notebooks. After creating these widgets, you can view the text values for the parameters **module_catalog** and **my_schema** at the top of the notebook. These values specify the module's catalog name and your specific schema.
-- MAGIC
-- MAGIC **NOTE**: Leaving the notebook can cause the widget values to be cleared. You will have to rerun the setup script and code below to reset the values. If you modify the widget values, please manually delete them and rerun the setup again to reset them.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Set widget values using the setup script
-- MAGIC dbutils.widgets.text("module_catalog",learner.catalog_name)
-- MAGIC dbutils.widgets.text("my_schema", learner.my_schema)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## a. Configure and Explore Your Environment
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Set the default catalog to **getstarted** and your unique schema. Then, view the available tables to confirm that no tables currently exist in your schema.

-- COMMAND ----------

-- Set the catalog and schema
USE CATALOG ${module_catalog};
USE SCHEMA IDENTIFIER(:my_schema);

-- Display available tables in your schema
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View the available files in your schema's **myfiles** volume. Confirm that only the **employees.csv** file is available.
-- MAGIC
-- MAGIC     **NOTE:** Remember, when referencing data in volumes, use the path provided by Unity Catalog, which always has the following format: */Volumes/catalog_name/schema_name/volume_name/*.

-- COMMAND ----------

LIST '/Volumes/${module_catalog}/${my_schema}/myfiles/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## b. Delta Lake Ingestion Techniques
-- MAGIC **Objective**: Create a Delta table from the **employees.csv**  file using various methods.
-- MAGIC
-- MAGIC - CREATE TABLE AS (CTAS)
-- MAGIC - UPLOAD UI (User Interface)
-- MAGIC - COPY INTO
-- MAGIC - AUTOLOADER (Overview only, outside the scope of this module)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE (CTAS)
-- MAGIC 1. Create a table from the **employees.csv** file using the CREATE TABLE AS statement similar to the previous demonstration. Run the query and confirm that the **current_employees_ctas** table was successfully created.

-- COMMAND ----------

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS current_employees_ctas;

-- Create the table using CTAS
CREATE TABLE current_employees_ctas
AS
SELECT ID, FirstName, Country, Role 
FROM read_files(
  '/Volumes/${module_catalog}/${my_schema}/myfiles/',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- Display available tables in your schema
SHOW TABLES;

-- COMMAND ----------

-- ALTERNATE METHOD: Create a temp view for the CTAS statement

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS current_employees_ctas;

-- Create temporary view
CREATE OR REPLACE TEMP VIEW vw_current_employees 
USING CSV
OPTIONS (
  path '/Volumes/${module_catalog}/${my_schema}/myfiles/',
  header = 'true',
  delimiter = ','
);

-- Use temporary view in CTAS statement to create the table
CREATE OR REPLACE TABLE current_employees_ctas AS
SELECT *
FROM vw_current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **current_employees_ctas** table and confirm that it contains 4 rows and 4 columns.

-- COMMAND ----------

SELECT *
FROM current_employees_ctas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPLOAD UI
-- MAGIC The add data UI allows you to manually load data into Databricks from a variety of sources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Complete the following steps to manually download the **employees.csv** file from your volume:
-- MAGIC - a. Select the Catalog icon ![catalog_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/catalog_icon.png) in the left navigation bar. 
-- MAGIC - b. Type the module's catalog name in the search bar (*getstarted*).
-- MAGIC - c. Select the refresh icon ![refresh_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/refresh_icon.png) to refresh the **getstarted** catalog.
-- MAGIC - d. Expand the **getstarted** catalog. Within the catalog, you should see a variety of schemas (databases).
-- MAGIC - e. Expand your schema. You can locate your schema in the setup notes in the first cell or in the top widget bar under the **my_schema** parameter. Notice that your schema contains **Tables** and **Volumes**.
-- MAGIC - f. Expand **Volumes** then **myfiles**. The **myfiles** volume should contain a single CSV file named **employees.csv**. 
-- MAGIC - g. Click on the kebab menu ![catalog_options](files/images/get-started-with-databricks-for-data-engineering-3.2.1/catalog_options.png) on the right-hand side of the **employees.csv** file and select **Download Volume file.** This will download the CSV file to your browser's download folder.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to manually upload the **employees.csv** file to your schema:
-- MAGIC - a. In the navigation bar select your schema. 
-- MAGIC - b. Select the **Create** drop down icon ![create_drop_down](files/images/get-started-with-databricks-for-data-engineering-3.2.1/create_drop_down.png), and select **Create table**.
-- MAGIC - c. Drop the **employees.csv** you downloaded earlier into the available section in the browser, or select **browse**, navigate to your downloads folder and select the **employees.csv** file.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Complete the following steps to create the Delta table using the UPLOAD UI.
-- MAGIC - a. In the UI confirm the table will be created in the catalog **getstarted** and your unique schema. 
-- MAGIC - b. Under **Table name**, name the table **current_employees_ui**.
-- MAGIC - c. Select the **Create table** icon at the bottom of the screen to create the table.
-- MAGIC - d. Confirm the table was created successfully. Then close out of the Catalog Explorer browser.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **Example**
-- MAGIC <br></br>
-- MAGIC ![create_table_ui](files/images/get-started-with-databricks-for-data-engineering-3.2.1/create_table_ui.png)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Use the SHOW TABLES statement to view the available tables in your schema. Confirm that the **current_employee_ui** table has been created. 
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Lastly, query the table to review its contents.
-- MAGIC
-- MAGIC **NOTE**: If you did not upload the table using the UPLOAD UI and name it **current_employees_ui** an error will be returned.

-- COMMAND ----------

SELECT * 
FROM current_employees_ui;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### COPY INTO
-- MAGIC Create a table from the **employees.csv** file using the [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) statement. The COPY INTO statement loads data from a file location into a Delta table. This is a retryable and idempotent operation — Files in the source location that have already been loaded are skipped. This is true even if the files have been modified since they were loaded

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create an empty table named **current_employees_copyinto** and define the column data types.

-- COMMAND ----------

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS current_employees_copyinto;

-- Create an empty table with the column data types
CREATE TABLE current_employees_copyinto (
  ID INT,
  FirstName STRING,
  Country STRING,
  Role STRING
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use the COPY INTO statement to load all files from the **myfiles** volume (currently only the **employees.csv** file exists) using the path provided by Unity Catalog. Confirm that the data is loaded into the **current_employees_copyinto** table.
-- MAGIC    
-- MAGIC     Confirm the following:
-- MAGIC     - **num_affected_rows** is 4
-- MAGIC     - **num_inserted_rows** is 4
-- MAGIC     - **num_skipped_correct_files** is 0

-- COMMAND ----------

COPY INTO current_employees_copyinto
  FROM '/Volumes/${module_catalog}/${my_schema}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Query the **current_employees_copyinto** table and confirm that all 4 rows have been copied into the Delta table correctly.

-- COMMAND ----------

SELECT * 
FROM current_employees_copyinto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Run the COPY INTO statement again and confirm that it did not re-add the data from the volume that was already loaded. Remember, COPY INTO is a retryable and idempotent operation — Files in the source location that have already been loaded are skipped.   
-- MAGIC - **num_affected_rows** is 0
-- MAGIC - **num_inserted_rows** is 0
-- MAGIC - **num_skipped_correct_files** is 0
-- MAGIC
-- MAGIC

-- COMMAND ----------

COPY INTO current_employees_copyinto
  FROM '/Volumes/${module_catalog}/${my_schema}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Run the script below to create an additional CSV file named **employees2.csv** in your **myfiles** volume. View the results and confirm that your volume now contains two CSV files: the original **employees.csv** file and the new **employees2.csv** file.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Create the new employees2.csv file in your volume
-- MAGIC learner.create_employees_csv2()
-- MAGIC
-- MAGIC ## Store the widget values in Python variables
-- MAGIC catalog_name = dbutils.widgets.get("module_catalog")
-- MAGIC my_schema = dbutils.widgets.get('my_schema')
-- MAGIC
-- MAGIC ## View the files in the your myfiles volume
-- MAGIC files = dbutils.fs.ls(f'/Volumes/{catalog_name}/{my_schema}/myfiles')
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Query the new **employees2.csv** file directly. Confirm that only 2 rows exist in the CSV file.

-- COMMAND ----------

SELECT ID, FirstName, Country, Role 
FROM read_files(
  '/Volumes/${module_catalog}/${my_schema}/myfiles/employees2.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Execute the COPY INTO statement again using your volume's path. Notice that only the 2 rows from the new **employees2.csv** file are added to the **current_employees_copyinto** table.
-- MAGIC
-- MAGIC - **num_affected_rows** is 2
-- MAGIC - **num_inserted_rows** is 2
-- MAGIC - **num_skipped_correct_files** is 0

-- COMMAND ----------

COPY INTO current_employees_copyinto
  FROM '/Volumes/${module_catalog}/${my_schema}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. View the updated **current_employees_copyinto** table and confirm that it now contains 6 rows, including the new data that was added.

-- COMMAND ----------

SELECT * 
FROM current_employees_copyinto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. View table's history. Notice that there are 3 versions.
-- MAGIC - **Version 0** is the initial empty table created by the CREATE TABLE statement.
-- MAGIC - **Version 1** is the first COPY INTO statement that loaded the **employees.csv** file into the Delta table.
-- MAGIC - **Version 2** is the second COPY INTO statement that only loaded the new **employees2.csv** file into the Delta table.

-- COMMAND ----------

DESCRIBE HISTORY current_employees_copyinto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### AUTOLOADER
-- MAGIC
-- MAGIC **NOTE: Auto Loader is outside the scope of this course.**
-- MAGIC
-- MAGIC Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup.
-- MAGIC
-- MAGIC ![autoloader](files/images/get-started-with-databricks-for-data-engineering-3.2.1/autoloader.png)
-- MAGIC
-- MAGIC The key benefits of using the auto loader are:
-- MAGIC - No file state management: The source incrementally processes new files as they land on cloud storage. You don't need to manage any state information on what files arrived.
-- MAGIC - Scalable: The source will efficiently track the new files arriving by leveraging cloud services and RocksDB without having to list all the files in a directory. This approach is scalable even with millions of files in a directory.
-- MAGIC - Easy to use: The source will automatically set up notification and message queue services required for incrementally processing the files. No setup needed on your side.
-- MAGIC
-- MAGIC Check out the documentation
-- MAGIC [What is Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) for more information.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## c. Cleanup
-- MAGIC 1. Drop your demonstration tables.

-- COMMAND ----------

DROP TABLE IF EXISTS current_employees_ctas;
DROP TABLE IF EXISTS current_employees_ui;
DROP TABLE IF EXISTS current_employees_copyinto;
DROP VIEW IF EXISTS vw_current_employees;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Drop the **employees2.csv** file.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Get the widget values into Python variables
-- MAGIC catalog_name = dbutils.widgets.get("module_catalog")
-- MAGIC my_schema = dbutils.widgets.get('my_schema')
-- MAGIC
-- MAGIC ## Remove employees2.csv from the myfiles volume
-- MAGIC dbutils.fs.rm(f"/Volumes/{catalog_name}/{my_schema}/myfiles/employees2.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
