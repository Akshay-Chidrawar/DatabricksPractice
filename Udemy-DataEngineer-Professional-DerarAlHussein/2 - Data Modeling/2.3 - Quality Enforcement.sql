-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');
DESCRIBE EXTENDED orders_silver;

--only 1 of 3 records to insert, fail the constraint. But still entire operation fails since Delta lake is ACID compliant. (Atomic transaction)
INSERT INTO orders_silver VALUES 
('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL);

ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;
DESCRIBE EXTENDED orders_silver;

-- COMMAND ----------

--Before adding a new constraint to the table, Databricks ensures all existing data satisfies the new constraint. Otherwise it will not allow to create the new constraint. 
ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);
DESCRIBE EXTENDED orders_silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
-- MAGIC
-- MAGIC query = (spark.readStream.table("bookstore_eng_pro.bronze")
-- MAGIC         .filter("topic = 'orders'")
-- MAGIC         .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
-- MAGIC         .select("v.*")
-- MAGIC         .filter("quantity > 0")
-- MAGIC      .writeStream
-- MAGIC         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
-- MAGIC         .trigger(availableNow=True)
-- MAGIC         .table("bookstore_eng_pro.orders_silver"))
-- MAGIC
-- MAGIC query.awaitTermination()

-- COMMAND ----------

drop table bookstore_eng_pro.orders_silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC delete_folder_if_exists('dbfs:/user/hive/warehouse/bookstore_eng_pro.db/orders_silver')
-- MAGIC delete_folder_if_exists('dbfs:/mnt/demo_pro/checkpoints/orders_silver')
