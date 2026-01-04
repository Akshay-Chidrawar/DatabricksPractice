# Databricks notebook source
directory = '/Volumes/workspace/default/managed_volume/ManishKumar/'

from pyspark.sql.functions import *
from pyspark.sql.types import *

import pandas as pd
import json

# COMMAND ----------

import pandas as pd

payload_pd = pd.read_json(directory+'json_data.json', lines=True)
users_bronze = pd.DataFrame(payload_pd).drop(columns='listings')
listings_bronze = pd.json_normalize(payload_pd,record_path='listings',meta=['user_id'])

print(type(payload_pd))
display(payload_pd)
display(users_bronze)
display(listings_bronze)

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Read Multi-line JSON").getOrCreate()

#payload = spark.read.option("multiline", "true").json("dbfs:/FileStore/json_data.json")
payload_df = spark.read.json(input_file)
payload_list = payload_df.collect()
print(payload_list)
print(type(payload_list))
users_bronze = pd.DataFrame(payload_pd).drop(columns='listings')
listings_bronze = pd.json_normalize(payload_pd,record_path='listings',meta=['user_id'])

print(users_bronze)
print(listings_bronze)

# COMMAND ----------

# DBTITLE 1,Pandas
# Load the JSON file
with open(directory+'json_data.json', 'r') as f:
    data = json.load(f)

# Display will not work on data, since it is a list of dictionaries, not a tabular dataframe.
print(type(data))
print(data)

# json_normalize() will create a pandas dataframe from above json structure.
df = pd.json_normalize(data)

df.display()
