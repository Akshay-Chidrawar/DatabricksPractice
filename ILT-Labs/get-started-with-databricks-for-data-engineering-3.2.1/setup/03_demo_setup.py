# Databricks notebook source
# MAGIC %run ./utility/learner_setup

# COMMAND ----------

# Setup the module's catalog and underlying files
learner = LearnerSetup(catalog_name="getstarted", create_employees_csv2=True)
