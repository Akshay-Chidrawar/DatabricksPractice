# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# DBTITLE 1,Define Source table
spark.sql (f"select * from {sourceTable}")

bronze_books = (
  spark.readStream
  .table(sourceTable)
  .filter("topic = 'books'")
)

bronze_books_cleansed = (
  bronze_books
  .select(F.from_json(F.col("value").cast("string"), changesTableSchema).alias("v"))
  .select("v.*")
)

bronze_books.createOrReplaceTempView('vw_bronze_books')
bronze_books_cleansed.createOrReplaceTempView('vw_bronze_books_cleansed')

display(spark.sql(f"select * from vw_bronze_books"))
display(spark.sql(f"select * from vw_bronze_books_cleansed"))


# COMMAND ----------

# DBTITLE 1,Current books
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books AS 
# MAGIC SELECT book_id, title, author, price
# MAGIC FROM books_silver
# MAGIC WHERE current IS TRUE;
# MAGIC
# MAGIC SELECT * FROM current_books
# MAGIC ORDER BY book_id;
