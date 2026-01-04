# Databricks notebook source
from pyspark.sql import SparkSession as SS

mySparkSession = SS\
    .builder\
    .appName('myappName')\
    .getOrCreate()

mySparkContext = mySparkSession.sparkContext

# COMMAND ----------

print(mySparkSession)
print(mySparkContext)
print(mySparkContext.appName) #Databricks manages the Spark context internally and overrides certain configurations to maintain consistency across notebooks and clusters. The default Spark session is pre-created with the name "Databricks Shell".Even if you try to create a new session, Databricks may return the existing one (due to getOrCreate()), which retains the original app name.

# mySparkSession.stop() - destroys connction to the cluster

# COMMAND ----------

spark
