# Databricks notebook source
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

setVars()

# COMMAND ----------

setVars()

# COMMAND ----------

spark.sql(f"""
          create table {tbl_silver_orders_customers} ({tblSchema_silver_orders_customers})
          """)

# COMMAND ----------

process_silver_orders_customers()
display(spark.sql(f"SELECT * FROM {tbl_silver_orders_customers} order by order_id,customer_id"))

# COMMAND ----------


display(spark.sql(f"SELECT * FROM {tbl_silver_customers} order by customer_id"))
display(spark.sql(f"SELECT * FROM {tbl_silver_orders} order by order_id,customer_id"))

# COMMAND ----------

load_new_data()
process_bronze()
process_silver_orders()
process_silver_customers()
process_silver_customers_orders()

display(spark.sql(f"SELECT * FROM {tbl5}"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank = 1")
                 .drop("rank", "_change_type", "_commit_version")
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

def process_customers_orders():
    orders_df = spark.readStream.table("orders_silver")
    
    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                       )

    query = (orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream
                    .foreachBatch(batch_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
process_customers_orders()
