# Databricks notebook source
# DBTITLE 1,Install & Import Python libraries
# MAGIC %pip install pymongo datetime sqlalchemy argparse
# MAGIC import pandas as pd
# MAGIC from pymongo import MongoClient
# MAGIC from datetime import datetime,timedelta
# MAGIC from IPython.display import display
# MAGIC import pyspark

# COMMAND ----------

!pip install --upgrade pip

# COMMAND ----------

# Install necessary libraries
%pip install snowflake-connector-python

# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize Spark session with Snowflake configurations
spark = SparkSession.builder \
    .appName("SnowflakeIntegration") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.3") \
    .getOrCreate()

# COMMAND ----------

# Install necessary libraries
%pip install snowflake-spark-connector
%pip install snowflake-connector-python

# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize Spark session with Snowflake configurations
spark = SparkSession.builder \
    .appName("SnowflakeIntegration") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.3") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %pip install net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.5.0
# MAGIC %pip install net.snowflake:snowflake-jdbc:3.13.3

# COMMAND ----------

# DBTITLE 1,Define MongoDB connection parameters
str_mongo_client = 'mongodb+srv://akshay10655028:rqYXPiOEdtHmz6vz@akmongodb.1frtg.mongodb.net/?retryWrites=true&w=majority&appName=AKMongoDB'
str_mongo_db = 'sample_mflix'
str_mongo_coll = 'comments'

mongo_client = MongoClient(str_mongo_client,tls=True,tlsAllowInvalidCertificates=True)
mongo_db = mongo_client[str_mongo_db]
mongo_coll = mongo_db[str_mongo_coll]

# COMMAND ----------

# DBTITLE 1,Date filters
today_ts = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
today_dt = today_ts.date()
filters={"date":{"$gte":today_ts + timedelta(days=-1),"$lt":today_ts}}

# COMMAND ----------

# DBTITLE 1,Flatten JSON data
payload = list(mongo_coll.find(filters))

df_payload = pd.DataFrame(payload)
display(df_payload)
biDataItems_raw = pd.DataFrame(payload).drop(columns='locations')
locations_raw = pd.json_normalize(payload,record_path='locations',meta=['_id'])


# COMMAND ----------

# DBTITLE 1,Raw data export
export_file_path = ''
biDataItems_raw.to_csv(inde)
