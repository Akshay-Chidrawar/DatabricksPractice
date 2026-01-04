# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
#dbutils.fs.rm('dbfs:/FileStore/ManishKumar/Flight_Data.csv')

# COMMAND ----------

Flight_Data = spark.read\
        .format('csv')\
        .options(header='True',inferschema='True')\
        .load('dbfs:/FileStore/ManishKumar/Flight_Data.csv')

# COMMAND ----------

Flight_Data.rdd.getNumPartitions()

# COMMAND ----------

partitioned_Flight_Data = Flight_Data\
    .repartition(8)

#make sure you use partitioned_Flight_Data (8 partitions) and not Flight_Data (only 1 partition). coalesce will not work if n > existing partitions. It will silently fail. 
coalesced_Flight_Data = partitioned_Flight_Data\
    .coalesce(3)

partitioned2_Flight_Data = partitioned_Flight_Data\
    .repartition(3)

Flight_Data.count()

# COMMAND ----------


Flight_Data\
    .groupBy(spark_partition_id())\
    .count()\
    .show()

partitioned_Flight_Data\
    .groupBy(spark_partition_id())\
    .count()\
    .show()

coalesced_Flight_Data\
    .groupBy(spark_partition_id())\
    .count()\
    .show()

partitioned2_Flight_Data\
    .groupBy(spark_partition_id())\
    .count()\
    .show()

# COMMAND ----------

files = dbutils.fs.ls('dbfs:/FileStore/ManishKumar')
for f in files:
    f_name = f.name
    f_size = round((f.size / (1024 * 1024)),2)
    print(f_name,f_size, "MB")
