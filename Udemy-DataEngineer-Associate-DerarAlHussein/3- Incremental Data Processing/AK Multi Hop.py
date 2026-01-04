# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets 

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("MultiHop").getOrCreate()

customers_lookup = (
    spark.read
    .format("json")
    .load(f"{dataset_bookstore}/customers-json")
    .createOrReplaceTempView("customers_lookup")
      )

# COMMAND ----------

ReadFromRaw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_ReadFromRaw")
        .load(f"{dataset_bookstore}/orders-raw")
)

RawToTransform = ReadFromRaw \
.withColumn("arrival_time", current_timestamp()) \
.withColumn("source_file", input_file_name())

WriteToBronze = RawToTransform \
.writeStream \
.trigger(processingTime='4 seconds')\
.format("delta")\
.option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_WriteTobronze")\
.outputMode("append")\
.table("orders_bronze")

# COMMAND ----------

WriteToSilver = 
(
spark
    .readStream
    .table("orders_bronze")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_ReadFromBronze")

    .join(
        customers_lookup
        ,orders_bronze.customer_id == customers_lookup.customer_id
        )
    .filter(col("quantity") > 0)
    .select(
        col("order_id"),
        col("quantity"),
        orders_bronze.customer_id,
        col("customers_lookup.profile.first_name").alias("f_name"),
        col("customers_lookup.profile.last_name").alias("l_name"),
        from_unixtime(col("order_timestamp"), 'yyyy-MM-dd HH:mm:ss').cast("timestamp").alias("order_timestamp"),
        col("books"))
    
    .writeStream
    .trigger(processingTime='4 seconds')
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_WriteToSilver")
    .outputMode("append")
    .table("orders_silver")

)


# COMMAND ----------

WriteToGold = 
(
spark
    .readStream
    .trigger(processingTime='4 seconds')
    .table("orders_silver")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_ReadFromSilver")
)

(
WriteToGold
.writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_WriteToGold")
    .outputMode("Complete")
    .table("orders_gold")
)
