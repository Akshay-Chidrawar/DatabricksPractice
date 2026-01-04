-- Databricks notebook source
--this variable should be configured as a parameter to pass to the DLT pipeline. It is declared here for us to keep track & not forget.
SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Bronze (streaming data for Orders, static data for Customers)

-- COMMAND ----------

--Create DLT using SQL version of Autoloader to load streaming data
CREATE OR REFRESH STREAMING TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS 
SELECT * 
FROM  cloud_files(
                "${datasets.path}/orders-json-raw"
                ,"json"
                ,map("cloudFiles.inferColumnTypes", "true")
                );

--Create DLT for static data
CREATE OR REFRESH MATERIALIZED VIEW customers
COMMENT "The customers lookup table, ingested from customers-json"
AS 
SELECT * 
FROM  json.`${datasets.path}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Silver (Join above 2, do some data transformations)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned 
(
CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
      cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
      c.profile:address:country as country
FROM  STREAM(LIVE.orders_raw) o
      LEFT JOIN LIVE.customers c ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Gold (Business aggregations)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
FROM LIVE.orders_cleaned
WHERE country = "China"
GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp);

CREATE OR REFRESH LIVE TABLE fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
FROM LIVE.orders_cleaned
WHERE country = "France"
GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp);
