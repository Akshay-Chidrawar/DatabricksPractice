-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Source & Target tables
-- MAGIC ####Source data

-- COMMAND ----------

USE default;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS author_counts;

CREATE OR REPLACE TEMP VIEW vw_books
USING CSV
OPTIONS (path="${dataset.bookstore}/books-csv",header = true, delimiter=";");

CREATE TABLE books AS
SELECT * FROM vw_books;

SELECT * FROM books;

SELECT author,count(book_id)
FROM books
GROUP BY all
ORDER BY author;

DELETE FROM books
WHERE book_id IN ('B16','B17','B18','B19','B20','B21');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Stream Reader
-- MAGIC 1. Stream source is a delta table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #view created below is like an alias to streamDF, except it is a SQL view and streamDF is streaming dataframe.
-- MAGIC streamDF = (
-- MAGIC       spark.readStream
-- MAGIC       .table("books")
-- MAGIC       .createOrReplaceTempView("books_streaming_tmp_vw") 
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Output

-- COMMAND ----------


--results to be written back to sink
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS 
(
SELECT author, count(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author
);

--read data from Stream Reader (Infinite run). Usually, streaming target (sink) is not queried or displayed since the associated query executes infinitely to see if there is any new data available. 

SELECT * FROM books_streaming_tmp_vw;
SELECT * FROM author_counts_tmp_vw;


/*
--below operations not supported in streaming DF/ view (sorting & depduplication)
 SELECT * 
 FROM books_streaming_tmp_vw
 ORDER BY author;
 */

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Stream Writer (infinite run)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC if 'streamQuery' in locals():
-- MAGIC     streamQuery.stop()
-- MAGIC
-- MAGIC if os.path.exists('dbfs:/mnt/demo/author_counts_stream_checkpoint'):
-- MAGIC       dbutils.fs.rm('dbfs:/mnt/demo/author_counts_stream_checkpoint')
-- MAGIC
-- MAGIC streamQuery = (
-- MAGIC       spark.table("author_counts_tmp_vw")
-- MAGIC       .writeStream
-- MAGIC       .trigger(processingTime='4 seconds')
-- MAGIC       .outputMode("complete")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_stream_checkpoint")
-- MAGIC       .table("author_counts")
-- MAGIC       )
-- MAGIC
-- MAGIC #use streamDF if you want to write back results from streamDF.
-- MAGIC #use spark.table() if you want to write back results from table or view. it accepts both table or view as input. 
-- MAGIC #Streaming & Static views are loaded as Streaming & Static data frames resp. by Spark. Thus, in case of streaming data, it must be specified at very beginning of data reading, in order to support streaming data writing. 
-- MAGIC
-- MAGIC #the "author_counts" is a persistent storage as a delta table. 
-- MAGIC #for aggregation streaming queries, we must specify outputMode=Complete , since results will be updated every time with new data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Data Validation (Writer - Stream mode)

-- COMMAND ----------

INSERT INTO books
values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
        ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
        ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

-- COMMAND ----------


SELECT * FROM author_counts 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stream Writer (Incremental Batch Mode)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Data Validation (Writer - Batch mode)

-- COMMAND ----------

INSERT INTO books
values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
        ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
        ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

-- COMMAND ----------

SELECT * FROM author_counts 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC if 'batchQuery' in locals():
-- MAGIC     batchQuery.stop()
-- MAGIC
-- MAGIC if os.path.exists('dbfs:/mnt/demo/author_counts_batch_checkpoint'):
-- MAGIC       dbutils.fs.rm('dbfs:/mnt/demo/author_counts_batch_checkpoint')
-- MAGIC
-- MAGIC BatchQuery = (
-- MAGIC       spark.table("author_counts_tmp_vw")                               
-- MAGIC       .writeStream        
-- MAGIC       .trigger(availableNow=True)
-- MAGIC       .outputMode("complete")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_batch_checkpoint")
-- MAGIC       .table("author_counts")
-- MAGIC       .awaitTermination()
-- MAGIC )
-- MAGIC
-- MAGIC #trigger(availableNow=True) will read & process all data that is currently available, & stop on completion.
-- MAGIC #awaitTermination() blocks the current thread, thus prevents any other cells of this notebook from executing, until this write operation is completed. We can specify timeout parameter in sec (default is indefinite). If query terminates before timeout, it returns boolean True, else False. Based on this outcome, we can decide next steps. 

-- COMMAND ----------

SELECT *
FROM author_counts

-- COMMAND ----------

DROP TABLE books;
DROP TABLE author_counts;
