-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##SQL Operations
-- MAGIC 1. Joins
-- MAGIC 2. Union, Intersect, Except (or Minus)
-- MAGIC 3. Pivot reshape

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_books 
USING CSV
OPTIONS (path="${dataset.bookstore}/books-csv",header = true, delimiter=";");

USE default;
CREATE OR REPLACE TABLE books AS
SElECT * FROM vw_books;

SElECT * FROM books;
SElECT * FROM orders_exploded;

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM orders_exploded o
INNER JOIN books b ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates;

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates;

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates;

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS
SELECT * FROM 
(
SELECT customer_id,
      book.book_id AS book_id,
      book.quantity AS quantity
FROM  orders_enriched
)PIVOT 
(
sum(quantity) 
FOR book_id in 
(
'B01', 'B02', 'B03', 'B04', 'B05', 'B06','B07', 'B08', 'B09', 'B10', 'B11', 'B12'
)
);

SELECT * FROM transactions;
