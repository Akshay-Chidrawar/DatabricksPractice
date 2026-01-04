# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %run ../Includes/Function_call

# COMMAND ----------

setVars()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC display(spark.sql(f"""
# MAGIC                   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC                   FROM {tbl_silver_orders_books}
# MAGIC                   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC                   """)))

# COMMAND ----------

display(spark.sql(f"""
                  SELECT *
                  FROM {tbl_silver_orders_books}
                  """))

display(spark.sql(f"""
                  SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count
                  FROM {tbl_silver_orders_books}
                  GROUP BY country, date_trunc("DD", order_timestamp)
                  """))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_stats_vw
# MAGIC WHERE country = "France"

# COMMAND ----------

from pyspark.sql import functions as F

query = (spark.readStream
        .table("books_sales")
        .withWatermark("order_timestamp", "10 minutes")
                 .groupBy(
                     F.window("order_timestamp", "5 minutes").alias("time")
                    ,"author")
                 .agg(
                     F.count("order_id").alias("orders_count"),
                     F.avg("quantity").alias ("avg_quantity"))
              .writeStream
                 .option("checkpointLocation", f"dbfs:/mnt/demo_pro/checkpoints/authors_stats")
                 .trigger(availableNow=True)
                 .table("authors_stats")
            )

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM authors_stats
