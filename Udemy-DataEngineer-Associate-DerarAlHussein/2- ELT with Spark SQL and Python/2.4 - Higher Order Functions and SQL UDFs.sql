-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Work on nested Array fields

-- COMMAND ----------


--apply filter on books.quantity as >=2
SELECT order_id, GrtThan1_copies FROM 
(
SELECT  order_id,
        books,
        FILTER (books, i -> i.quantity >= 2) AS GrtThan1_copies
FROM    orders
)
WHERE size(GrtThan1_copies) > 0;

--transform books.subtotal as *0.8 (discounted value)
SELECT  order_id,
        books,
        TRANSFORM (books, b -> CAST(b.subtotal * 0.8 AS INT)) AS subtotal_after_discount
FROM    orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

--arguments are optional. we take 2nd portion of the split (the [1] indicates 2nd portion starting with [0])
CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING
RETURN concat("https://www.", split(email, "@")[1]);

DESCRIBE FUNCTION get_url;
DESCRIBE FUNCTION EXTENDED get_url;

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;



-- COMMAND ----------

SELECT email
, get_url(email) domain
, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
