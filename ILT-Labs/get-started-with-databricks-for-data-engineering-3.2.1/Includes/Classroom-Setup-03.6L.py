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

DA.paths.kafka_events = f"{DA.paths.datasets}/ecommerce/raw/events-kafka"

spark.sql("set spark.databricks.delta.copyInto.formatCheck.enabled = false")
spark.sql(f"DROP TABLE IF EXISTS events_json")
spark.sql(f"CREATE TABLE events_json")
spark.sql(f"""   
  COPY INTO events_json
  FROM '{DA.paths.kafka_events}'
  FILEFORMAT = JSON
  COPY_OPTIONS ('mergeSchema' = 'true');
  """)
    
# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS events_json
#   (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)
# USING JSON 
# OPTIONS (path = "{DA.paths.kafka_events}")
# """)

DA.conclude_setup()
