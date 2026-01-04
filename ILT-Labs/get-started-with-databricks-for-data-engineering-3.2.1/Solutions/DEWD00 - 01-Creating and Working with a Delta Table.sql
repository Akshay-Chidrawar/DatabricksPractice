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
-- MAGIC ## Demo 01 - Creating and Working with a Delta Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (required)
-- MAGIC To set up your lab environment, execute the following scripts. Be sure to review the environment information provided, and take note of the module's catalog name (**getstarted**) and your unique schema name.

-- COMMAND ----------

-- MAGIC %run ./setup/01_demo_setup

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
-- MAGIC ## a. Explore your Catalog
-- MAGIC
-- MAGIC **The Unity Catalog object model**
-- MAGIC
-- MAGIC In Unity Catalog, all metadata is registered in a metastore. The hierarchy of database objects in any Unity Catalog metastore is divided into three levels, represented as a three-level namespace (catalog.schema.table-etc) when you reference tables, views, volumes, models, and functions.
-- MAGIC
-- MAGIC ![unity_catalog_object_model](files/images/get-started-with-databricks-for-data-engineering-3.2.1/unity_catalog_object_model.png)
-- MAGIC
-- MAGIC For more information check out [What is Unity Catalog?](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Complete the following to manually view the module catalog **getstarted** and your schema:
-- MAGIC - a. Select the Catalog icon ![catalog_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/catalog_icon.png) in the left navigation bar. 
-- MAGIC - b. Type the module's catalog name in the search bar (*getstarted*).
-- MAGIC - c. Select the refresh icon ![refresh_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/refresh_icon.png) to refresh the **getstarted** catalog.
-- MAGIC - d. Expand the **getstarted** catalog. Within the catalog, you should see a variety of schemas (databases).
-- MAGIC - e. Find your specific schema. You can locate your schema in the setup notes in the first cell or in the top widget bar under the **my_schema** parameter. 
-- MAGIC - f. Expand your schema. Notice that your schema only contains a volume named **myfiles**.
-- MAGIC - g. Expand your volume. The volume should contain a single CSV file named **employees.csv**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Execute the code cell to set the current catalog to **getstarted** and the current schema to your specific schema. This configuration avoids the need to use a two-level naming convention (catalog.schema) in your queries. The SELECT statement will display the name of your current catalog and schema. 
-- MAGIC
-- MAGIC     **NOTE**: You must use [IDENTIFIER clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-names-identifier-clause.html#identifier-clause) to parse strings as object identifiers for schemas(databases), tables, views, functions, columns, and fields. This will dynamically use your specific schema with the **my_schema** parameter. Supported in Runtime 13.3 LTS and above.
-- MAGIC
-- MAGIC

-- COMMAND ----------

USE CATALOG ${module_catalog};
USE SCHEMA IDENTIFIER(:my_schema);

-- View your set catalog and schema
SELECT 
  current_catalog() as current_catalog, 
  current_schema() as current_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE:** Moving forward you will want to migrate your parameter syntax. You can ignore this note.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use the [DESCRIBE SCHEMA EXTENDED](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-schema.html) statement to display the metadata and properties of your schema.

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED IDENTIFIER(:my_schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Use the [SHOW TABLES](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-tables.html) statement to display the available tables in your schema. You will notice that there are currently no tables available in your schema.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Use the [SHOW VOLUMES](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-volumes.html) statement to list all of the volumes available in your schema. Notice that you have a single volume available, **myfiles**.
-- MAGIC
-- MAGIC
-- MAGIC     **NOTE**: Volumes are Unity Catalog objects representing a logical volume of storage in a cloud object storage location. Volumes provide capabilities for accessing, storing, governing, and organizing files. While tables provide governance over tabular datasets, volumes add governance over non-tabular datasets. 
-- MAGIC
-- MAGIC     You can use volumes to store and access files in any format, including structured, semi-structured, and unstructured data.
-- MAGIC
-- MAGIC     [What are Unity Catalog volumes?](https://docs.databricks.com/en/volumes/index.html)

-- COMMAND ----------

SHOW VOLUMES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Use the LIST statement to list all of the files in the **myfiles** volume. Notice that the volume only has the **employees.csv** file. It contains a information about current employees. 
-- MAGIC
-- MAGIC     When interacting with data in volumes, you use the path provided by Unity Catalog, which always has the following format: */Volumes/catalog_name/schema_name/volume_name/*.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC     For more information on exploring directories and data files managed with Unity Catalog volumes, check out the [Explore storage and find data files](https://docs.databricks.com/en/discover/files.html)  documentation.
-- MAGIC
-- MAGIC     **NOTE**: (Python) *dbutils.fs.ls("/Volumes/catalog_name/schema_name/volume_name/")*
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

LIST '/Volumes/${module_catalog}/${my_schema}/myfiles';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## b. Create a Delta Table from a CSV File
-- MAGIC
-- MAGIC All saved tables on Databricks are Delta tables by default. Whether you’re using Apache Spark DataFrames or SQL, you get all the benefits of Delta Lake just by saving your data to the lakehouse with default settings.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Query the **employees.csv** file directly using SQL to view the file. Notice that the query returns a view of the CSV file with the headers as the first row of data.
-- MAGIC
-- MAGIC     **NOTE**: This syntax is specific to Spark SQL and allows you to read files directly without explicitly loading them into a table first. You specify the file format and enclose the file path in **backticks**. This method works for various file types.
-- MAGIC
-- MAGIC     *SELECT * FROM \<file_format>.\`/path/to/file`;*

-- COMMAND ----------

SELECT *
FROM csv.`/Volumes/${module_catalog}/${my_schema}/myfiles/employees.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. You can use the *text* file format to view files as strings in a column. This enables you to view the contents of the file. Notice that the first row contains the column names and the fields are separated by commas.

-- COMMAND ----------

SELECT * 
FROM text.`/Volumes/${module_catalog}/${my_schema}/myfiles/employees.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. With SQL you can use the [read_files](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html) table-valued function to read the CSV file into tabular form and apply specific options to modify how the file is read. 
-- MAGIC
-- MAGIC     Execute the query and confirm that the results show 4 employees with valid column names.
-- MAGIC
-- MAGIC     **NOTE**: A rescued data column is provided by default to rescue any data that doesn’t match the schema.

-- COMMAND ----------

SELECT *
FROM read_files(
  '/Volumes/${module_catalog}/${my_schema}/myfiles/employees.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Use the CREATE TABLE AS (CTAS) statement with the query from above to create a Delta table in the Lakehouse named **current_employees** using the **employees.csv** file. The table will be created in your schema.
-- MAGIC
-- MAGIC     **NOTE:** The USING DELTA statement is optional. The CREATE TABLE statement will create a Delta table by default.

-- COMMAND ----------

-- Drop the table if it already exists for demonstration purposes
DROP TABLE IF EXISTS current_employees;

-- Create a Delta table using the CSV file
CREATE TABLE current_employees USING DELTA
AS
SELECT ID, FirstName, Country, Role 
FROM read_files(
  '/Volumes/${module_catalog}/${my_schema}/myfiles/employees.csv',
  format => 'csv',
  header => true,
  inferSchema => true
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Complete the following steps to manually view the table in your schema::
-- MAGIC - a. Select the Catalog icon ![catalog_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/catalog_icon.png) in the left navigation bar. 
-- MAGIC - b. Type the catalog name (*getstarted*) in the search bar.
-- MAGIC - c. Select the refresh icon ![refresh_icon](files/images/get-started-with-databricks-for-data-engineering-3.2.1/refresh_icon.png)to refresh the catalog.
-- MAGIC - d. Expand the **getstarted** catalog. Within the catalog you should see a variety of schemas (databases).
-- MAGIC - e. Find your schema. You can locate your schema in the setup notes in the first cell, or in the top widget bar under the *my_schema* parameter. 
-- MAGIC - f. Expand your schema. Notice that your schema contains **Volumes** and **Tables**.
-- MAGIC - g. Expand **Tables**. Confirm that the **current_employees** Delta table is available.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC (optional) To create a Delta table from a CSV file using Python, you can use the code below:
-- MAGIC
-- MAGIC **NOTE:** To uncomment code, highlight the commented lines and press **ALT + /** (or **COMMAND + /** on Mac).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC # #
-- MAGIC # # Get widget parameters values in Python
-- MAGIC # #
-- MAGIC # catalog = dbutils.widgets.get('module_catalog')
-- MAGIC # my_schema = dbutils.widgets.get('my_schema')
-- MAGIC
-- MAGIC
-- MAGIC # #
-- MAGIC # # Read the CSV file and create a Spark DataFrame
-- MAGIC # #
-- MAGIC # sdf = (spark.read
-- MAGIC #        .format("csv")
-- MAGIC #        .option('header', 'true')
-- MAGIC #        .option('inferSchema','true')
-- MAGIC #        .load(f'/Volumes/{catalog}/{my_schema}/myfiles/employees.csv')
-- MAGIC #     )
-- MAGIC # sdf.display()
-- MAGIC
-- MAGIC
-- MAGIC # #
-- MAGIC # # Create a Delta table from the Spark DataFrame
-- MAGIC # #
-- MAGIC
-- MAGIC # (sdf
-- MAGIC #  .write
-- MAGIC #  .mode("overwrite")
-- MAGIC #  .format("delta")
-- MAGIC #  .saveAsTable(f"{catalog}.{my_schema}.current_employees_py")
-- MAGIC # )
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Use the SHOW TABLES statement to display all the tables in your schema. You should see a table named **current_employees**.
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Query the **current_employees** table to view the data. Confirm that it contains 4 rows of data with a list of current employees.

-- COMMAND ----------

SELECT * 
FROM current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Use the DESCRIBE DETAIL statement to view detailed information about the **current_employees** table. View the results. 
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC     - The **format** column indicates that the **current_employees** table has been created as a Delta table.
-- MAGIC     - The **location** column displays the cloud location of the table in the following format for AWS: 
-- MAGIC         - *s3://\<bucket-name>/\<metastore id>/tables/\<table id>* 
-- MAGIC
-- MAGIC     **NOTES:** 
-- MAGIC     - The results of the DESCRIBE DETAIL statement includes additional information about the Delta table. For more details, refer to the [Detail schema documentation](https://docs.databricks.com/en/delta/table-details.html#detail-schema).
-- MAGIC     - For more information on how paths work for data managed by Unity Catalog check out the [How do paths work for data managed by Unity Catalog?](https://docs.databricks.com/en/data-governance/unity-catalog/paths.html) documentation.

-- COMMAND ----------

DESCRIBE DETAIL current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Use the [DESCRIBE EXTENDED](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html) statement to display detailed information about the specified columns and additional metadata for the Delta table.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC     - The top portion of the results displays column metadata of the table.
-- MAGIC     - Scroll down the **col_name** column to find the *Type* value. Notice that the **data_type** specifies *Managed*. This indicates that Databricks manages the lifecycle and file layout for the table. Managed tables are the default table creation method.
-- MAGIC
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Execute the [DESCRIBE HISTORY](https://docs.databricks.com/en/sql/language-manual/delta-describe-history.html) statement on the **current_employees** table to retrieve Delta table history.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC     - The **version** column indicates the table is on version 0
-- MAGIC     - The **timestamp** column displays when the table was created
-- MAGIC     - The **operation** column shows what operation was performed.
-- MAGIC     - The **operationsMetrics** column display information about the number of files, number of output rows, and number of output bytes.

-- COMMAND ----------

DESCRIBE HISTORY current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## c. Insert, Update and Delete Records in the Delta Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the current **current_employees** table. 
-- MAGIC
-- MAGIC     Notice the following in the Delta table:
-- MAGIC     - There are 4 columns and 4 rows
-- MAGIC     - The **ID** values *1111*, *2222*, *3333*, *4444* are in the table
-- MAGIC     - The  **Role** of **ID** *1111* is *Manager*

-- COMMAND ----------

SELECT * 
FROM current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Perform the following operations on the **current_employees** Delta table:
-- MAGIC   - 1. Insert two new employees named *Alex* and *Sanjay*. 
-- MAGIC   - 2. Update the **Role** of employee **ID** *1111* to *Senior Manager*.
-- MAGIC   - 3. Delete the record of the employee with the **ID** *3333*.

-- COMMAND ----------

-- 1. Insert two employees into the table
INSERT INTO current_employees 
VALUES
    (5555, 'Alex','USA', 'Instructor'),
    (6666, 'Sanjay','India', 'Instructor');

-- 2. Update a record in the table
UPDATE current_employees
  SET Role = 'Senior Manager'
  WHERE ID = 1111;

-- 3. Delete a record in the table
DELETE FROM current_employees
  WHERE ID = 3333;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. View the data in the **current_employees** table. Notice that the table has been modified from the original version.

-- COMMAND ----------

SELECT *
FROM current_employees
ORDER BY ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Each operation that modifies a Delta Lake table creates a new table version. View the history of the table.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC   - The table has versions 0 through 4. 
-- MAGIC   - **Version 0** is the original table that was created.
-- MAGIC   - **Version 1** contains the WRITE operation that inserted two new employees.
-- MAGIC   - **Version 2** contains the UPDATE operation that modified the job role.
-- MAGIC   - **Version 3** contains the DELETE operation that removed an employee.
-- MAGIC   - **Version 4** contains the OPTIMIZE operation on the table. Predictive optimization is a feature in Delta Lake on Databricks that automatically optimizes Delta tables (optional feature). For more information, view the [Predictive optimization for Delta Lake
-- MAGIC ](https://docs.databricks.com/en/optimizations/predictive-optimization.html) documentation.
-- MAGIC
-- MAGIC **NOTE:** The OPTIMIZE operation might be in a different order.

-- COMMAND ----------

DESCRIBE HISTORY current_employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## d. Use Time Travel to Read Previous Versions of the Delta Table
-- MAGIC You can use history information to audit operations, rollback a table, or query a table at a specific point in time using time travel.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the current version of the **current_employees** table. By default, the most recent version of the table will be used.

-- COMMAND ----------

SELECT *
FROM current_employees
ORDER BY ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use time travel to view the table prior to the DELETE operation. Notice that the results show 6 employees before the record was deleted.
-- MAGIC
-- MAGIC     **NOTE**: Time travel takes advantage of the power of the Delta Lake transaction log to access data that is no longer in the table.

-- COMMAND ----------

SELECT *
FROM current_employees VERSION AS OF 2
ORDER BY ID;

-- Alternate syntax
-- SELECT *
-- FROM current_employees@v2
-- ORDER BY ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. View the original table using version 0. Notice that the original 4 employees are displayed.

-- COMMAND ----------

SELECT *
FROM current_employees VERSION AS OF 0
ORDER BY ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## e. Drop the Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Drop the Delta tables created in this demonstration.

-- COMMAND ----------

DROP TABLE IF EXISTS current_employees;
DROP TABLE IF EXISTS current_employees_py;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
