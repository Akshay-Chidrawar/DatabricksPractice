# Databricks notebook source
directory = '/Volumes/workspace/default/sample_data/ManishKumar/'

from pyspark.sql.functions import *
from pyspark.sql.types import *

import pandas as pd
import json

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
