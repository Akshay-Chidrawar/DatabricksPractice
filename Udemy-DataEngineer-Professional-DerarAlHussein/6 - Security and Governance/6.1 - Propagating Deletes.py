# Databricks notebook source
# MAGIC %run /Shared/Databricks-Certified-Data-Engineer-Professional/Includes/Function_call

# COMMAND ----------

setVars()

# COMMAND ----------

display(spark.sql(f"select * from {tbl_bronze_customers} where row_status='delete'"))

# COMMAND ----------


tbl_DeleteRequests = 'bookstore_eng_pro.delete_requests'
checkpoint_path = 'dbfs:/mnt/demo_pro/checkpoints/delete_requests'

from pyspark.sql import functions as F

query = (
  spark.readStream
  .table(tbl_bronze_customers)  
  .filter("row_status = 'delete'")
  .select("customer_id"
          ,"request_timestamp"
          ,F.date_add("request_timestamp", 30).alias("deadline") 
          ,F.lit("requested").alias("status"))
  .writeStream
  .outputMode("append")
  .option("checkpointLocation",checkpoint_path)
  .trigger(availableNow=True)
  .table(tbl_DeleteRequests)

)

# COMMAND ----------

from pyspark.sql import functions as F

(spark.readStream
        .table("bronze")
        .filter("topic = 'customers'")
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*", F.col('v.row_time').alias("request_timestamp"))
        .filter("row_status = 'delete'")
        .select("customer_id", "request_timestamp",
                F.date_add("request_timestamp", 30).alias("deadline"), 
                F.lit("requested").alias("status"))
    .writeStream
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/delete_requests")
        .trigger(availableNow=True)
        .table("delete_requests")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM customers_silver
# MAGIC WHERE customer_id IN (SELECT customer_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", 2)
                 .table("customers_silver"))

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes"))

    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM customers_orders
        WHERE customer_id IN (SELECT customer_id FROM deletes)
    """)
    
    microBatchDF._jdf.sparkSession().sql("""
        --AKshay SQL
        UPDATE delete_requests r                      
        SET r.status = "deleted"
        where r.customer_id in (select d.customer_id from deletes d)

        --Derar's SQL                         
        --MERGE INTO delete_requests r
        --USING deletes d
        --ON d.customer_id = r.customer_id
        --WHEN MATCHED
          --THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

(deleteDF.writeStream
         .foreachBatch(process_deletes)
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/deletes")
         .trigger(availableNow=True)
         .start())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders@v6
# MAGIC EXCEPT
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", 2)
           .table("customers_silver")
           .filter("_change_type = 'delete'"))
display(df)
