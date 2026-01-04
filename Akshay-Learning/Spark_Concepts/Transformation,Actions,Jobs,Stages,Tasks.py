# Databricks notebook source
# MAGIC %md
# MAGIC ### Separate jobs are created for below actions while reading CSV file:
# MAGIC - spark.read --> scan entire dataset from source, understands the structure of data in terms of columns (number & name) and loads it into a Dataframe. If header = True is specified, the first row is identified as column headers within the same scan itself. Thus, no separate job is created for header. This is required to construct the DataFrame object with an appropriate column structure.
# MAGIC - inferschema --> If True, scans file again & deduces data type for each column. If False, no additional job is created. 
# MAGIC
# MAGIC ### Transformations happening internally (Spark UI):
# MAGIC
# MAGIC spark.read --> 
# MAGIC - read the data & save to RDD (not partitioned)
# MAGIC - create a new partitioned RDD from above.
# MAGIC - apply a transformation on above RDD to: 
# MAGIC   - identify rows with help of new lines & create new Row object for each line.
# MAGIC   - identify columns with help of delimiters & create new columns.
# MAGIC
# MAGIC inferschema -->
# MAGIC - read the data & save to RDD (not partitioned)
# MAGIC - create a new partitioned RDD from above.
# MAGIC - apply one or more transformations on above RDD to identify data types.
# MAGIC
# MAGIC ### observations in Jobs:
# MAGIC
# MAGIC even though CSV read file job is already completed, those results are not cached in memory or stored on disc. Thus, above code snippet will again read CSV file. 
# MAGIC
# MAGIC Greyed out stages are skipped because Spark reuses results from previous stages created in earlier jobs; no need to recompute again. This is called Shuffle file reuse/ Stage reuse. 
# MAGIC
# MAGIC ### AQE
# MAGIC - by default, AQE is enabled. 
# MAGIC - change it using **spark.conf.set("spark.sql.adaptive.enabled", "false")**
# MAGIC - when Spark optimizer cannot resolve all statistics during compile time, it triggers new jobs at each shuffle exchange point to gather statistics, allows AQE to observe shuffle stats, optimizes execution plan if possible, and then runs final job to execute optimized plan. Spark will not execute anything in initial jobs.
# MAGIC
# MAGIC ### Count of Partitions
# MAGIC - by default, partition count = 200. 
# MAGIC - change it using **spark.conf.set("spark.sql.shuffle.partitions, n")**
# MAGIC
# MAGIC ### Databricks specific observations
# MAGIC
# MAGIC repartition()/groupBy() triggers its own job with even though it is a transformation. This is an exceptional case since .this is an eager wide transformation and involves shuffling, thus Spark decides to prepare data first before proceeding. However, if .repartition() is used alone without .show(), no separate job is created. 
# MAGIC
# MAGIC Other exceptions where extra jobs are created - 
# MAGIC cache() or persist() --> materialize cache.
# MAGIC checkpoint() --> persist RDD/ dataframe to storage.
# MAGIC broadcast join --> materialize broadcast table. 
# MAGIC
# MAGIC #################################################################################
# MAGIC
# MAGIC - Spark optimizes above 2 filter transformations as: (Predicate Pushdown) -->
# MAGIC DEST_COUNTRY_NAME==United States AND (ORIGIN_COUNTRY_NAME==India OR ORIGIN_COUNTRY_NAME==Singapore)
# MAGIC
# MAGIC - Projection Pruning --> Include only those columns which are required. 
# MAGIC
# MAGIC - If you write first group by, next filter; Spark will reverse the order of execution for these operations for optimization. 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Read + Inferschema
Flight_Data = spark.read\
        .format('csv')\
        .options(header='True',inferschema='True')\
        .load('dbfs:/FileStore/ManishKumar/Flight_Data.csv')

# COMMAND ----------

# DBTITLE 1,collect()
print(
    Flight_Data\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .collect()
)

# COMMAND ----------

# DBTITLE 1,AQE disabled - partition count 200
# Diable AQE to avoid separate job for groupBy. 
# Default count of partitions is 200. Thus 200 tasks created for shuffle/ exchange stage.

spark.conf.set("spark.sql.adaptive.enabled", "false")

print(
    Flight_Data\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .groupBy('ORIGIN_COUNTRY_NAME').agg(sum('count').alias('TotalCount'))\
    .collect()
    )

# COMMAND ----------

# DBTITLE 1,AQE disabled - partition count 2
#Set partition count as 2.

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", 2)

print(
    Flight_Data\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .groupBy('ORIGIN_COUNTRY_NAME').agg(sum('count').alias('TotalCount'))\
    .collect()
    )

# COMMAND ----------

# DBTITLE 1,AQE enabled
#AQE is enabled by default. 
print(
    Flight_Data\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .groupBy('ORIGIN_COUNTRY_NAME').agg(sum('count').alias('TotalCount'))\
    .collect()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Repartition & GroupBy

# COMMAND ----------

# DBTITLE 1,AQE disabled, partition count 2

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", 2)

print(
    Flight_Data\
    .repartition(4)\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .groupBy('ORIGIN_COUNTRY_NAME').agg(sum('count').alias('TotalCount'))\
    .collect()
    )

#WITH AQE DISABLED:
# Job1 (3 stages) --> CSV read (Stage1) + repartition (Stage2) + filter>select>groupBy>agg (Stage3)

# repartition --> read will fetch source data into 4 partitions
# shuffle.partitions= 2 --> groupBy will shuffle data into 2 partitions


# COMMAND ----------

# DBTITLE 1,AQE enabled
spark.conf.set("spark.sql.adaptive.enabled", "true")

print(
    Flight_Data\
    .repartition(4)\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .groupBy('ORIGIN_COUNTRY_NAME').agg(sum('count').alias('TotalCount'))\
    .collect()
    )

#WITH AQE ENABLED:

#Job1 (1 stage - 1 task) --> read csv + repartition

#Job2 (2 stages - 4 tasks) --> filter>select>groupBy>agg: Based on static estimates (default partitions count as 200, default join as shuffle sort merge; without knowing actual data size or skewness) Spark reads data from Exchange from previous stage, skips its own stage.

#Job3 (3 stages) -->  collect() triggers the real job where AQE skips its own first 2 stages & compares static estimates v/s realtime stats, decides to optimize the plan and run optimized execution plan, instead of earlier plan (It coalesced partition count from 2 to 1). 


# COMMAND ----------

# DBTITLE 1,explain plan
Flight_Data\
    .repartition(2)\
    .filter(col('DEST_COUNTRY_NAME')=='United States')\
    .filter((col('ORIGIN_COUNTRY_NAME')=='India')| (col('ORIGIN_COUNTRY_NAME')=='Singapore'))\
    .select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','count')\
    .groupBy('ORIGIN_COUNTRY_NAME').agg(sum('count').alias('TotalCount'))\
    .explain(extended=True)
