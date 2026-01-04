# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

setVars()

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {tbl_silver_customers}"))
enableCDFonTable(tbl_silver_customers)
display(spark.sql(f"DESCRIBE HISTORY {tbl_silver_customers}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###CDF #1

# COMMAND ----------

load_new_data()
process_bronze_customers()
process_silver_customers()

# COMMAND ----------

displayHTML("""
            <h5> Bronze after CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_bronze_customers} where customer_id='C00165' order by customer_id, create_ts desc"))
displayHTML("""
            <h5> Silver before CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_silver_customers} @v1 where customer_id='C00165' order by customer_id"))
displayHTML("""
            <h5> Silver after CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_silver_customers} where customer_id='C00165' order by customer_id,last_updated desc"))
displayHTML("""
            <h5> Capture CDF from Silver </h5> 
            """)
display(spark.sql(f"select * from table_changes('{tbl_silver_customers}', 2) where customer_id='C00165' order by customer_id,last_updated desc"))
displayHTML("""
            <h5> Version History </h5> 
            """)
display(spark.sql(f"DESCRIBE HISTORY {tbl_silver_customers}"))
# every MERGE operation generates a new version of the table as observed below: process_silver_customers()

# COMMAND ----------

# MAGIC %md
# MAGIC ###CDF #2

# COMMAND ----------

load_new_data()
process_bronze_customers()
process_silver_customers()

# COMMAND ----------

displayHTML("""
            <h5> Bronze after CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_bronze_customers}@v1 where customer_id='C00045' order by customer_id, source_file,row_time desc"))
displayHTML("""
            <h5> Bronze after CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_bronze_customers} where customer_id='C00045' order by customer_id, row_time desc"))
displayHTML("""
            <h5> Silver before CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_silver_customers}@v1 where customer_id='C00045' order by customer_id"))
displayHTML("""
            <h5> Silver after CDF </h5> 
            """)
display(spark.sql(f"select * from {tbl_silver_customers} where customer_id='C00045' order by customer_id"))

# COMMAND ----------

cdf_df = (
  spark.read 
  .format("delta")
  .option("readChangeData", True)
  .option("startingVersion", 2)
  .table(tbl_silver_customers))

display(cdf_df.orderBy(F.col('customer_id').asc(),F.col('last_updated').desc()))

# use read or readstream

# COMMAND ----------

files = dbutils.fs.ls(tblStorage_silver_customers)
display(files)

# all parquet files in /_change_data
files = dbutils.fs.ls(tblStorage_silver_customers+"/_change_data")
display(files)
