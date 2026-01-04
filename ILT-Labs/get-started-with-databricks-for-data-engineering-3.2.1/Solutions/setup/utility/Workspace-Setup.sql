-- Databricks notebook source
-- MAGIC %md
-- MAGIC CREATE AND GRANT PERMISSIONS TO CATALOG FOR COURSE.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS getstarted;
GRANT USE CATALOG ON CATALOG getstarted TO `account users`;
GRANT CREATE SCHEMA ON CATALOG getstarted TO `account users`;
