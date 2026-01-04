# Databricks notebook source
data_source_uri = "s3://dalhussein-courses/datasets/bookstore/v1/"
dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
data_catalog = 'hive_metastore'
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

import os

#check weather file exists or not.
def folder_exists(target_folder):
  os.path.exists(target_folder)

def download_dataset(source, target):
  source_folders = dbutils.fs.ls(source)
  for f in source_folders:
    source_folder = f"{source}/{f.name}"
    target_folder = f"{target}/{f.name}"
    if not folder_exists(target_folder):
      print(f"Copying {f.name} ...")
      dbutils.fs.cp(source_folder, target_folder, True)

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1
  
def set_current_catalog(catalog_name):
  spark.sql(f"USE CATALOG {catalog_name}")



# COMMAND ----------

dbutils.notebook.exit('Exit');

# COMMAND ----------

download_dataset(data_source_uri, dataset_bookstore)
set_current_catalog(data_catalog)

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_bookstore}/orders-streaming"
raw_dir = f"{dataset_bookstore}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# DLT
streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
streaming_books_dir = f"{dataset_bookstore}/books-streaming"

raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
raw_books_dir = f"{dataset_bookstore}/books-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1
