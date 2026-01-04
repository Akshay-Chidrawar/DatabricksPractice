# Databricks notebook source
print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # These titles are added to notebook table of contents.
# MAGIC ## Title Large
# MAGIC ### Title Medium
# MAGIC #### Title Small
# MAGIC
# MAGIC **bold text** 
# MAGIC *italicized text*
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC Sample Image:
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC Sample Table:
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Sample Embedded HTML: 
# MAGIC <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank">Notebooks documentation</a>
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,%run
# MAGIC %run ../Includes/Setup
# MAGIC print(full_name)

# COMMAND ----------

# DBTITLE 1,%fs
# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

# DBTITLE 1,dbutils
dbutils.help()
dbutils.fs.help() #interact with DBFS

#using dbutils is better because you can use this as a part of python script, whereas %fs is not possible in Python.
files = dbutils.fs.ls('/databricks-datasets')
display(files) #display() is better than print() for readability. it also allows file download as csv, however it can only preview 1000 records. when running SQL queries, results will always be shown in a tabular format using display() function by default.
