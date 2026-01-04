# Databricks notebook source
# MAGIC %run /Shared/Databricks-Certified-Data-Engineer-Professional/Includes/Function_call

# COMMAND ----------

setVars()
cleanup_and_setup()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initial Load

# COMMAND ----------

process_bronze_books()
process_silver_books()
process_silver_currentbooks()

process_bronze_customers()
process_silver_customers()
enableCDFonTable(tbl_silver_customers)

process_bronze_orders()
process_silver_orders()

process_silver_orders_books()
process_silver_orders_customers()

# COMMAND ----------

display(spark.sql(f"select * from {tbl_bronze_books}"))
display(spark.sql(f"select * from {tbl_silver_books}"))
display(spark.sql(f"select * from {tbl_silver_currentbooks}"))

display(spark.sql(f"select * from {tbl_bronze_customers}"))
display(spark.sql(f"select * from {tbl_silver_customers}"))

display(spark.sql(f"select * from {tbl_bronze_orders}"))
display(spark.sql(f"select * from {tbl_silver_orders}"))

display(spark.sql(f"select * from {tbl_silver_orders_books}"))
# display(spark.sql(f"select * from {tbl_silver_orders_customers}"))

# COMMAND ----------

dbutils.notebook.exit('Notebook execution completed successfully.')

# COMMAND ----------

# MAGIC %md
# MAGIC ##TESTING

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view jv
# MAGIC using json
# MAGIC options(path='dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw/01.json');
# MAGIC
# MAGIC select topic,cast(unbase64(value) as string) from jv
# MAGIC where topic='customers';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view jv
# MAGIC using json
# MAGIC options(path='dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-streaming/02.json');
# MAGIC
# MAGIC select topic,cast(unbase64(value) as string) from jv
# MAGIC where topic='customers';

# COMMAND ----------

display(spark.sql(f"select * from {tbl_bronze_customers}"))
display(spark.sql(f"select * from {tbl_silver_customers}"))

# COMMAND ----------

import inspect

funcDef = inspect.getsource(scdType1_upsert)
print(funcDef)
