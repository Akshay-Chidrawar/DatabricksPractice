# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

setVars()

# COMMAND ----------


#bronze table at intial stage (300 records for customers)
display(spark.sql(f"select * from {tbl_bronze_customers} order by key"))
#silver customers table at intial stage (207 records - after exclude Delete operations.)
display(spark.sql(f"select * from {tbl_silver_customers} order by customer_id,last_updated desc"))


# COMMAND ----------

count = spark.table(tbl_silver_customers).count()
expected_count = spark.table(tbl_silver_customers).select("customer_id").distinct().count()

assert count == expected_count
print("Unit test passed.",count,expected_count)
