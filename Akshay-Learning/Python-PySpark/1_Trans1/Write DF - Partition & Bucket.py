# Databricks notebook source
# MAGIC %md
# MAGIC ## Dataframe Writer API
# MAGIC - data is written into number of output files <= with count of partitions in dataframe, because each partition is written by an Executor in parallel. It may happen that one or more partitions are empty and thus no file is written. This is the default behaviour. You can check your partition size using built in Spark function spark_partition_id as follows: df.groupBy(spark_partition_id()).count().show()
# MAGIC - If you want to change this setup, you can repartition your data into desired number of partitions and accordingly output files get generated. 2 ways to implement this:
# MAGIC     - repartition --> blind partitioning, remains in memory. maxRecordsPerFile(n) controls Partition size (n is number of rows). Do not create partitions more than available record count. Those will remain empty and processing overhead. 
# MAGIC     - partitionBy --> logical partitioning based on key column, remains on disc, folder level.
# MAGIC     - bucketBy --> logical partitioning based on pre defined buckets, folder level. works only for managed tables. 
# MAGIC
# MAGIC Partitioning and bucketing are the approaches to write back data to your storage systems. They essentially decide in what way the data is organized on your storage. 
# MAGIC
# MAGIC ### Refer notebook for Repartition & Coalesce. 
# MAGIC
# MAGIC ### .partitionBy() & .save()
# MAGIC - when data files are stored using this approach, they dont save the key column in their data because that would be redundant information to store, since those columns are already available in folder structure.
# MAGIC - partition by year, gender.
# MAGIC - discrete values, columns with low cardinality.
# MAGIC - pass path value in .save() where dataframe needs to be stored. It works as a file system storage. 
# MAGIC
# MAGIC ### .bucketBy() & .saveAsTable()
# MAGIC - bucketBy() cannot use save() because bucketBy() works only for managed tables & not dataframes. 
# MAGIC - saveAsTable() saves dataframe as a table and stores table details in Hive metastore. 
# MAGIC - bucket by age group, exam score.
# MAGIC - continuous values as intervals, columns with high cardinality
# MAGIC - recommended to keep data sorted in buckets to improve join performance. 
# MAGIC - user pass only (number of buckets, column name); Spark decides bucket boundaries internally.
# MAGIC - Before applying bucketBy on your data, always repartition it: df.repartition(num_buckets, "bucket_column")
# MAGIC
# MAGIC ### Advantages of bucket
# MAGIC
# MAGIC - for wide transformations like joins, buckets helps to partition tables optimally; thus avoid data shuffling.   
# MAGIC condition: both tables must have same bucketing on join column.
# MAGIC
# MAGIC - bucket pruning: When searching for rows with specific criteria, Spark intelligently identifies which buckets to look into. Eg- ID numbers 1 to 1000 are bucketed into 10 buckets:
# MAGIC     bucket0 --> 1 to 100
# MAGIC     bucket1 --> 101 to 200 and so on.
# MAGIC If Spark is searching for ID = 420, it identifies the appropriate bucket number (ID / bucket size):
# MAGIC bucket4 --> 401 to 500 
# MAGIC Thus, it only scans 100 records instead of 1000. 
# MAGIC
# MAGIC ### .format()
# MAGIC - csv
# MAGIC
# MAGIC ### .options()
# MAGIC - header
# MAGIC
# MAGIC ### .mode()
# MAGIC -   append: append new data to existing table.
# MAGIC -   overwrite: overwrite existing table. 
# MAGIC -   error if exists: throw error if table already exists.
# MAGIC -   ignore: abort write operation silently if table already exists.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read CSV
employee_data_new = spark.read\
    .format('csv')\
    .options(header='true', inferSchema='true')\
    .load('dbfs:/FileStore/ManishKumar/employee_data_new.csv')
employee_data_new.show(3)

# COMMAND ----------

# DBTITLE 1,Write CSV - without repartition
employee_data_new.write\
    .options(header='true')\
    .mode('overwrite')\
    .format('csv')\
    .save('dbfs:/FileStore/ManishKumar/employee_data_new')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/ManishKumar/employee_data_new'))

# COMMAND ----------

# DBTITLE 1,Write CSV - with repartition
employee_data_new.repartition(3).write\
    .options(header='true')\
    .mode('overwrite')\
    .format('csv')\
    .save('dbfs:/FileStore/ManishKumar/employee_data_new')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/ManishKumar/employee_data_new'))

# COMMAND ----------

# DBTITLE 1,Read above written dataframe from storage
employee_data_new_read = spark.read\
    .format('csv')\
    .options(header='true')\
    .load('dbfs:/FileStore/ManishKumar/employee_data_new')

employee_data_new_read.show()

# COMMAND ----------

# DBTITLE 1,apply partitionBy
employee_data_new.write\
    .options(header='true')\
    .partitionBy('address','gender')\
    .mode('overwrite')\
    .format('csv')\
    .save('dbfs:/FileStore/ManishKumar/employee_data_new')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/ManishKumar/employee_data_new'))
display(dbutils.fs.ls('dbfs:/FileStore/ManishKumar/employee_data_new/address=INDIA/'))

# COMMAND ----------

# DBTITLE 1,apply bucketBy
employee_data_new.write\
    .options(header='true',path='dbfs:/FileStore/ManishKumar/employee_data_new_bucket')\ #(optional) this is the physical storage location where dataframe is stored. 
    .bucketBy(3,'id')\
    .mode('overwrite')\
    .format('csv')\
    .saveAsTable('employee_data_new_bucket') # (optional) this is the logical table reference to the dataframe stored (3 level table name). 
    
    #If physical location is not specified, Dataframe is stored at a location, as per the table name specified in .saveAsTable() method. If logical reference is not specified, Dataframe will not be available for SQL query. One among the 2 must be specified in order to store the dataset. If both are specified, physical storage and logical reference of the table will be as per what is mentioned in these 2 arguments. 
