# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = True,
                             requires_uc = True,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()

# COMMAND ----------

import os, time, shutil, sqlite3
import pandas as pd

# Create a user-specific copy of the sales-csv.
DA.paths.sales_csv = f"{DA.paths.working_dir}/sales-csv"
dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/sales-csv", DA.paths.sales_csv, True)

start = int(time.time())
print(f"Creating tables used in this lesson", end="...")

# DA.paths.ecommerce_db = f"{DA.paths.working_dir}/ecommerce.db"
# datasource_path = f"{DA.paths.datasets}/ecommerce/raw/users-historical"

# # Create the temp directory and declare the path to the temp db file.
# db_temp_dir = f"/tmp/{DA.username}"
# dbutils.fs.mkdirs(f"file:{db_temp_dir}")
# db_temp_path = f"{db_temp_dir}/ecommerce.db"

DA.clone_source_table("users", f"{DA.paths.datasets}/ecommerce/delta", "users_hist")
DA.clone_source_table("events_update", f"{DA.paths.datasets}/ecommerce/delta")
# # Move the temp db to the final location
# dbutils.fs.mv(f"file:{db_temp_path}", DA.paths.ecommerce_db)
# DA.paths.ecommerce_db = DA.paths.ecommerce_db.replace("dbfs:/", "/dbfs/")

# # Report on the setup time.
# total = spark.read.parquet(datasource_path).count()
# print(f"({int(time.time())-start} seconds / {total:,} records)")

# Create table used for cloning operations
spark.sql(f"""CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`{DA.paths.datasets}/ecommerce/raw/events-historical`""")

# Another cloning table
spark.sql(f"""CREATE OR REPLACE TABLE historical_sales_bronze AS
SELECT * FROM parquet.`{DA.paths.datasets}/ecommerce/raw/sales-historical`;""")

# Report on the setup time.
# total = spark.read.parquet(datasource_path).count()
# print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

DA.conclude_setup()
