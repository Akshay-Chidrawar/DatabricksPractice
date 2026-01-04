-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream
-- MAGIC         .format("cloudFiles")
-- MAGIC         .option("cloudFiles.format", "parquet")
-- MAGIC         .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_ReadFromRaw")
-- MAGIC         .load(f"{dataset_bookstore}/orders-raw")
-- MAGIC         .createOrReplaceTempView("tempvw_orders_ReadFromRaw")
-- MAGIC )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tmpvw_orders_ReadFromRaw_Transform AS
(
SELECT *, current_timestamp() arrival_time, input_file_name() source_file
FROM tempvw_orders_ReadFromRaw
);

SELECT * FROM tmpvw_orders_ReadFromRaw_Transform;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("tmpvw_orders_ReadFromRaw_Transform")
-- MAGIC       .writeStream
-- MAGIC       .format("delta")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_tmpvw_orders_WriteTobronze")
-- MAGIC       .outputMode("append")
-- MAGIC       .table("orders_bronze"))

-- COMMAND ----------

SELECT count(*) FROM orders_bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Creating Static Lookup Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC       .format("json")
-- MAGIC       .load(f"{dataset_bookstore}/customers-json")
-- MAGIC       .createOrReplaceTempView("customers_lookup"))

-- COMMAND ----------

SELECT * FROM customers_lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Silver Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC   .table("orders_bronze")
-- MAGIC   .createOrReplaceTempView("orders_bronze_tmp"))

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
  FROM orders_bronze_tmp o
  INNER JOIN customers_lookup c
  ON o.customer_id = c.customer_id
  WHERE quantity > 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("orders_enriched_tmp")
-- MAGIC       .writeStream
-- MAGIC       .format("delta")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
-- MAGIC       .outputMode("append")
-- MAGIC       .table("orders_silver"))

-- COMMAND ----------

SELECT * FROM orders_silver

-- COMMAND ----------

SELECT COUNT(*) FROM orders_silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Gold Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC   .table("orders_silver")
-- MAGIC   .createOrReplaceTempView("orders_silver_tmp"))

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM orders_silver_tmp
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Batch mode Write

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("daily_customer_books_tmp")
-- MAGIC       .writeStream
-- MAGIC       .format("delta")
-- MAGIC       .outputMode("complete")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
-- MAGIC       .trigger(availableNow=True)
-- MAGIC       .table("daily_customer_books"))

-- COMMAND ----------

SELECT * FROM daily_customer_books

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Stopping active streams

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for s in spark.streams.active:
-- MAGIC     print("Stopping stream: " + s.id)
-- MAGIC     s.stop()
-- MAGIC     s.awaitTermination()
