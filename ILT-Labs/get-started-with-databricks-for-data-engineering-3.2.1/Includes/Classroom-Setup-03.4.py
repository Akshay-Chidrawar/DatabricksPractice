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
DA.conclude_setup()

# COMMAND ----------

# Create a user-specific copy of the sales-csv.
DA.paths.sales_csv = f"{DA.paths.working_dir}/sales-csv"
dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/sales-csv", DA.paths.sales_csv, True)

# Create a user-specific copy of the sales-csv.
DA.paths.users_parquet = f"{DA.paths.working_dir}/ecommerce/raw/users-30m"
dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/users-30m", DA.paths.users_parquet, True)

# COMMAND ----------


