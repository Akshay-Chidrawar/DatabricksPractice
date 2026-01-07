# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### JSON file
# MAGIC - collection of records; each record is **enclosed within {}**; thus it is a dictionary data structure.
# MAGIC - each record is a group of **one or more key value pairs separated by commas**.
# MAGIC - Based on how records are separated from each other, below types of JSON files exist: 
# MAGIC
# MAGIC ### line delimited json / SingleLine json
# MAGIC - each line represents a complete record.
# MAGIC - Thus, no need to put commas at the end of record (it gives NULL records at EOF while reading file). 
# MAGIC - Also, no need to enclose within []. (file is not read properly, DIY)
# MAGIC - if each record has exact same columns, no issues. However, if they don't, Spark will create extra columns in the dataframe. Remember, JSON being a semi structured data, is a flexible format to store data; however, when you convert it to tabular format, it will introduce redundancy for such flexible data. 
# MAGIC
# MAGIC ### multiLine json 
# MAGIC - each line represents only 1 key value pair of a record.
# MAGIC - entire block must be enclosed within [] to treat as a list and records must be separated by commas. If [] not passed, Spark reads only 1st record and stops. 
# MAGIC
# MAGIC ### Read JSON files in Spark:
# MAGIC - By default, Spark treats any input JSON file as line delimited JSON. If input file is multi line, specify the same using below syntax: **.options(multiline='True')**
# MAGIC - Performance wise, reading a line delimited json file is better than reading multi line json file; 
# MAGIC - Reason: In the latter case, Spark treats entire file as a single object and requires to parse the file in order to identify the records hierarchy.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Initialize
directory = '/Volumes/workspace/default/sample_data/ManishKumar/'

from pyspark.sql.functions import *
from pyspark.sql.types import *

import pandas as pd
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### SingleLine JSON

# COMMAND ----------

# DBTITLE 1,Legitimate
SingleLine = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive')\
    .load(directory+'SingleLine.json')
display(SingleLine)

# COMMAND ----------

# DBTITLE 1,Corrupted
SingleLine_corrupted1a = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive')\
    .load(directory+'SingleLine_corrupted1a.json')
display(SingleLine_corrupted1a)

SingleLine_corrupted1b = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive')\
    .load(directory+'SingleLine_corrupted1b.json')
display(SingleLine_corrupted1b)

SingleLine_corrupted1c = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive')\
    .load(directory+'SingleLine_corrupted1c.json')
display(SingleLine_corrupted1c)


# COMMAND ----------

# DBTITLE 1,extra field in a record
df = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive')\
    .load(directory+'singleLine_extraField.json')

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### multiLine JSON

# COMMAND ----------

# DBTITLE 1,Legitimate
df = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive',multiLine='true')\
    .load(directory+'multiLine.json')

df.show()

# COMMAND ----------

# DBTITLE 1,corrupted
df = spark.read.format('json')\
    .options(header= 'true',inferSchema='true',mode='permissive',multiLine='true')\
    .load(directory+'multiLine_corrupted.json')

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### nested JSON

# COMMAND ----------

# DBTITLE 1,Schema of json_data
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# StructType --> list of individual StructField
# StructField --> dict
# 

json_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("listings", ArrayType(
        StructType([
            StructField("description", StringType(), True),
            StructField("listing_id", StringType(), True),
            StructField("place", StructType([
                StructField("Area", StringType(), True),
                StructField("City", StringType(), True)
            ]), True),
            StructField("services", ArrayType(
                StructType([
                    StructField("service_id", StringType(), True),
                    StructField("service_provider", StringType(), True),
                    StructField("service_type", StringType(), True)
                ])
            ), True)
        ])
    ), True),
    StructField("user_id", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,PySpark
payload = spark.read.format('json')\
    .options(header=True
             ,inferSchema=True
             ,multiline=True
             ,mode='permissive')\
    .load(directory+'json_data.json')

# you can use schema = json_schema instead of inferSchema=True, which infers schema manually (consumes time & resources).

payload = payload.select("user_id","first_name","listings")
payload.display()

users = payload\
    .select('user_id','first_name')
users.display()

#use explode() when you encounter array; use "." operator when you encounter nested fields.

properties = payload\
    .withColumn('Properties',explode(col('listings')))\
    .select(
        col('Properties.listing_id').alias('Property_id')
        ,col('Properties.description').alias('description')
        ,col('Properties.place.Area').alias('place_Area')
        ,col('Properties.place.City').alias('place_City')
        )
properties.display()

services = payload\
    .withColumn('Properties',explode(col('listings')))\
    .withColumn('Services',explode('Properties.services'))\
    .select(
        col('Services.service_id').alias('Service_id')
        ,col('Services.service_type').alias('Service_type')
        ,col('Services.service_provider').alias('Service_provider')
    )
services.display()

user_properties_services_relator = payload\
    .withColumn('Properties',explode(col('listings')))\
    .withColumn('Services',explode('Properties.services'))\
    .select(
        col('user_id').alias('user_id')
        ,col('Properties.listing_id').alias('Property_id')
        ,col('Services.service_id').alias('Service_id')
    )
user_properties_services_relator.display()

full_data = user_properties_services_relator\
    .join(users,['user_id'])\
    .join(properties,['Property_id'])\
    .join(services,['Service_id'])\
    .select('user_id','first_name','Property_id','description','place_Area','place_City','Service_id','Service_type','Service_provider')\
    .orderBy('user_id','Property_id','Service_id')
full_data.display()
