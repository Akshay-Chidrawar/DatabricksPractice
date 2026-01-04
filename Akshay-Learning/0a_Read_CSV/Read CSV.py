# Databricks notebook source
# MAGIC %md
# MAGIC # spark.read (DataFrameReader API)
# MAGIC
# MAGIC ## **OPTIONAL PARAMETERS:**
# MAGIC
# MAGIC ### .format() 
# MAGIC - Default is parquet. 
# MAGIC - Also supports csv, json, JDBC/ ODBC (table), etc. 
# MAGIC
# MAGIC ### .option("key","value") OR .options("key1"="value1","key2"="value2")
# MAGIC - pass key value pairs. Examples:
# MAGIC - inferschema (default false --> All columns inferred as String).
# MAGIC - header (default false --> First row also considered as part of data, not column header).
# MAGIC - Mode 
# MAGIC   1. permissive --> (default) allows corrupted data and set them as NULL.  
# MAGIC   2. dropmalformed --> drop corrupted data and continue operation.
# MAGIC   3. Failfast --> terminate operation as soon as corrupted data encountered.
# MAGIC
# MAGIC ### .schema() 
# MAGIC - user can provide custom schema here for Spark to apply this custom schema on the data.
# MAGIC - either provide inferschema = true OR provide custom schema in .schema(). If you provide both, .schema() takes precedence. 
# MAGIC
# MAGIC ## **MANDATORY PARAMETERS:**
# MAGIC
# MAGIC ### .load()
# MAGIC - path of input file which needs to be read. 

# COMMAND ----------

# MAGIC %md
# MAGIC PRACTICAL

# COMMAND ----------

file_path = 's3://s3bucket-databrickslearning/Files/Flight_Data.csv'

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,TimestampType

#schema is not being enforced properly; Nullable flag is not working. (Structfield syntax and Spark SQL syntax both).

custom_schema = StructType([
  StructField('DEST_COUNTRY_NAME', StringType(), nullable=False),
  StructField('ORIGIN_COUNTRY_NAME', StringType(), nullable=False),
  StructField('count', IntegerType(), nullable=True),
  StructField('Last_flight_date', DateType(), nullable=True)
])

custom_schema_sql = """
DEST_COUNTRY_NAME STRING NOT NULL,
ORIGIN_COUNTRY_NAME STRING NOT NULL,
count INT,
Last_flight_date TIMESTAMP
"""

"""
NOTES
Threw error when Last_flight_date in CSV file was MM-DD-YYYY format. When changed to YYYY-MM-DD, error got resolved.

"""

# COMMAND ----------

df1=spark.read\
    .format('csv')\
    .load(file_path)

df2 = spark.read\
    .options(header='true', mode='failfast')\
    .schema(custom_schema)\
    .format('csv')\
    .load(file_path)

df3 = spark.read\
    .options(header='true', mode='failfast')\
    .schema(custom_schema_sql)\
    .format('csv')\
    .load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### .columns()
# MAGIC - returns list of columns.
# MAGIC - this information is already stored, not computed on call; also does not print. Thus, it is an attribute, not a method (printSchema). An attribute does not perform any explicit computation or action; a method does. 
# MAGIC
# MAGIC ### .printSchema() 
# MAGIC - prints schema of dataframe: data type, NULL allowed or not.
# MAGIC
# MAGIC ### .display() 
# MAGIC - displays dataframe in tabular format.
# MAGIC - allows user to:
# MAGIC   - download data in CSV/ Excel. 
# MAGIC   - create vizualizations on data, apply filters, select columns, search data, etc.
# MAGIC
# MAGIC ### .show()
# MAGIC
# MAGIC ### OPTIONAL PARAMETERS
# MAGIC
# MAGIC 1st parameter (n int) 
# MAGIC - how many rows to show.
# MAGIC - default: n = 20.
# MAGIC
# MAGIC 2nd parameter (truncate boolean or int) 
# MAGIC - weather or not to truncate column values/ how many characters to show.
# MAGIC - default: truncate = True and truncates column values at length 20.
# MAGIC - applicable for all columns.
# MAGIC
# MAGIC 3rd parameter (vertical boolean) 
# MAGIC - weather or not to show datframe in vertical format (one line per column value).
# MAGIC - default: vertical=False
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df1.printSchema()
df1.show()

df2.printSchema()  
df2.show(2,3)

df3.printSchema()
df3.show(n=3,vertical=True)
df3.display() # OR display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Corrupted data (when do we say a record is corrupted/ malformed?)
# MAGIC - CSV --> extra value coming in record.
# MAGIC - JSON --> curly bracket (open or close) is missing.

# COMMAND ----------

employee_schema = StructType([
  StructField('id', IntegerType(), True)
  ,StructField('name', StringType(), True)
  ,StructField('age', IntegerType(), True)
  ,StructField('salary', IntegerType(), True)
  ,StructField('address', StringType(), True)
  ,StructField('nominee', StringType(), True)
  # ,StructField('_corrupted_record', StringType(), True)
])

# COMMAND ----------

file_path = 's3://s3bucket-databrickslearning/Files/employee_data.csv.txt'
bad_records_path = 's3://s3bucket-databrickslearning/Files/employee_data_bad_records/'

employee_df1 = spark.read\
    .options(header='true', mode='PERMISSIVE')\
    .format('csv')\
    .load(file_path)

employee_df2 = spark.read\
    .options(header='true', mode='DropMalformed')\
    .format('csv')\
    .load(file_path)

employee_df3 = spark.read\
    .options(header='true', mode='FailFast')\
    .format('csv')\
    .load(file_path)

employee_df4 = spark.read\
    .options(header='true', mode='permisssive')\
    .schema(employee_schema)\
    .format('csv')\
    .load(file_path)

employee_df5 = spark.read\
    .options(header='true',badRecordsPath=bad_records_path)\
    .schema(employee_schema)\
    .format('csv')\
    .load(file_path)

# COMMAND ----------

employee_df1.show() #records 4,5 are included inspite of being corrupted, record 5 has address value missing, thus set as NULL. However, this is not a corrupted record.
employee_df2.show() #records 3,4 are excluded since they are corrupted.
employee_df3.show() #fails since dataframe contains corrupted records.
employee_df4.show() #field "_corrupted_record" helps identify which records are corrupt <> NULL.
employee_df5.show() #we cannot pass "mode" when using "badRecordsPath". It takes default as mode=DropMalformed.

# COMMAND ----------

bad_records = spark.read\
    .format('json')\
    .load('s3://s3bucket-databrickslearning/Files/employee_data_bad_records/20250717T121732/bad_records/part-00000-37aba4fd-9122-4a07-b404-e698d4219fb9')
bad_records.select('record').show(truncate=False)
