-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ingest and Manipulate a Delta Table
-- MAGIC
-- MAGIC This notebook provides a hands-on review of some of the features Delta Lake brings to the data lakehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (required)
-- MAGIC To set up your lab environment, execute the following scripts. Be sure to review the environment information provided, and take note of the module's catalog name (**getstarted**) and your unique schema name.

-- COMMAND ----------

-- MAGIC %run ./setup/05_demo_setup

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
-- MAGIC ## Begin Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Set your current Catalog to **getstarted** and your schema to your specific schema. You can either:
-- MAGIC - Hardcode the Catalog and schema values.
-- MAGIC - Use parameters from the widgets.
-- MAGIC
-- MAGIC
-- MAGIC **HINT**: The parameters can be specified using the following in SQL:
-- MAGIC - Catalog: *${module_catalog}*
-- MAGIC - Schema: *IDENTIFIER(:my_schema)*

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run a query to view your current Catalog and schema. Verify that the results show the module's Catalog (**getstarted**) and your specific schema.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. View the available volumes in your schema and confirm that the **taxi_files** volume is listed.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. List the files in the **taxi_files** volume and check the **name**  column to determine the file types stored in the volume. Ignore any additional files that begin with an underscore (_).
-- MAGIC
-- MAGIC **HINT**: Use the following path format to access the volume: */Volumes/catalog_name/schema_name/volume_name/*.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Query the volume path directly and preview the data in the file using the appropriate file format. Make sure to use backticks around the path to your volume.
-- MAGIC
-- MAGIC **HINT**: SELECT * FROM \<file-format\>. \`\<path-to-volume\>\`

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Create a table in your schema called **taxitrips_bronze** that contains the following columns:
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | tpep_pickup_datetime | TIMESTAMP |
-- MAGIC | tpep_dropoff_datetime | TIMESTAMP |
-- MAGIC | trip_distance | DOUBLE |
-- MAGIC | fare_amount | DOUBLE |
-- MAGIC | pickup_zip | INT |
-- MAGIC | dropoff_zip | INT |
-- MAGIC
-- MAGIC **NOTE:** The DROP TABLE statement will drop the table if it already exists to avoid errors.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Use the [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) statement to populate the table with files from the **taxi_files** volume into the **taxitrips_bronze** table. Include the following options:
-- MAGIC - FILEFORMAT = '\<file-format\>'
-- MAGIC - FORMAT_OPTIONS
-- MAGIC   - 'header' = 'true'
-- MAGIC   - 'inferSchema' = 'true'

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Count the number of rows in the **taxitrips_bronze** table. Confirm that the table has 21,932 rows.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. View the **taxitrips_bronze** table's history. Confirm version 0 and version 1 are available.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Run the following script to delete all rows where **trip_distance** is less than *1*. Confirm *5,387* rows were deleted.

-- COMMAND ----------

DELETE FROM taxitrips_bronze
  WHERE trip_distance < 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. View the **taxitrips_bronze** table's history. View the **operation** column. View the version where the *DELETE* operation occurred.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Run a query to count the total number of rows in the current version of the **taxitrips_bronze** table. Confirm that the current table contains *16,545* rows.
-- MAGIC
-- MAGIC **HINT:** By default the most recent version will be used.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Query the original version of the table. Confirm that the original table contains *21,932* rows.
-- MAGIC
-- MAGIC **HINT:** FROM \<table> VERSION AS OF \<n>

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CHALLENGE**
-- MAGIC
-- MAGIC
-- MAGIC 14. Whoops! You made a mistake and didn't mean to delete the rows from earlier. Use the [RESTORE](https://docs.databricks.com/en/sql/language-manual/delta-restore.html) statement to restore a Delta table to the original state prior to the *DELETE* operation.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15. View the history of the **taxitrips_bronze** table. Confirm the most recent version contains the **operation** *RESTORE*.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 16. Count the total number of rows in the current **taxitrips_bronze** table. Confirm that the most recent version of the table contains *21,932* rows.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 17. Drop the **taxitrips_bronze** table.

-- COMMAND ----------

-- TODO 
-- <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Summary
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands
-- MAGIC * Reviewing table metadata including table history
-- MAGIC * Leverage Delta Lake versioning for snapshot queries and rollbacks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
