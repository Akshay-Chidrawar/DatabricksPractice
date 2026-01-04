# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

setVars()

# COMMAND ----------

# DBTITLE 1,Join result before new data load
process_silver_orders_books()
display(spark.sql(f"select * from {tbl_silver_orders_books}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Static table modified (No change in Join result)

# COMMAND ----------

# DBTITLE 1,Static table modified (No change in Join result)
load_new_data()
process_bronze_books()
process_silver_books()
process_silver_currentbooks()

# COMMAND ----------

# DBTITLE 1,Count = 776
process_silver_orders_books()
display(spark.sql(f"SELECT * FROM {tbl_silver_orders_books}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Stream table modified (Join result changes)

# COMMAND ----------

# DBTITLE 1,Stream table modified (Join result changes)
process_bronze_orders()
process_silver_orders()

# COMMAND ----------

# DBTITLE 1,Count = 1547
process_silver_orders_books()
display(spark.sql(f"SELECT * FROM {tbl_silver_orders_books}"))
