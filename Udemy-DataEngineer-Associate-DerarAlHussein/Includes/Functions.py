# Databricks notebook source
def remove_directory_if_exists(path):
    try:      
        dbutils.fs.ls(path) # Check if the directory exists        
        dbutils.fs.rm(path, recurse=True) # If no exception is raised, the directory exists, so remove it
        print(f"Directory removed successfully. {path}")
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            print(f"Directory does not exist. {path}")
        else:
            raise

hive = 'dbfs:/user/hive/warehouse/'
ext = 'dbfs:/ext'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sample_tbl
# MAGIC (
# MAGIC eid INT
# MAGIC ,ename STRING
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE sample_tbl ADD CONSTRAINT eid_pk PRIMARY KEY (eid);
# MAGIC ALTER TABLE sample_tbl ADD CONSTRAINT ename_notnull (ename NOT NULL);
