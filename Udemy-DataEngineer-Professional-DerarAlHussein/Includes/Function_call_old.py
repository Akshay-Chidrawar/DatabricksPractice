# Databricks notebook source
from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone","Asia/Kolkata")

# COMMAND ----------

source_path = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/'
kafka_streaming = source_path+'kafka-streaming/'
kafka_raw = source_path+'kafka-raw/'
ItemToRetain = kafka_raw+'01.json'
books_updates_streaming = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/books-updates-streaming'
books_updates_raw = kafka_raw+'books-updates/'

storage_path = 'dbfs:/user/hive/warehouse/'
checkpoint_path = 'dbfs:/mnt/demo_pro/checkpoints/'
lookup_country_path = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/country_lookup'

dbname = 'bookstore_eng_pro'
tblID1 = 'bronze'
tblID11 = 'bronze_books'
tblID12 = 'bronze_customers'
tblID13 = 'bronze_orders'
tblID21 = 'silver_books'
tblID22 = 'silver_customers'
tblID23 = 'silver_orders'
tblID24 = 'silver_orders_books'
tblID25 = 'silver_orders_customers'

tbl11 = dbname+'.'+tblID11
tbl12 = dbname+'.'+tblID12
tbl13 = dbname+'.'+tblID13
tbl21 = dbname+'.'+tblID21
tbl22 = dbname+'.'+tblID22
tbl23 = dbname+'.'+tblID23
tbl24 = dbname+'.'+tblID24
tbl25 = dbname+'.'+tblID25

dbStorage = storage_path+dbname+'.db/'

tbl11Storage = dbStorage+tblID11
tbl12Storage = dbStorage+tblID12
tbl13Storage = dbStorage+tblID13
tbl21Storage = dbStorage+tblID21
tbl22Storage = dbStorage+tblID22
tbl23Storage = dbStorage+tblID23
tbl11Checkpoint = checkpoint_path+tblID11
tbl12Checkpoint = checkpoint_path+tblID12
tbl13Checkpoint = checkpoint_path+tblID13
tbl21Checkpoint = checkpoint_path+tblID21
tbl22Checkpoint = checkpoint_path+tblID22
tbl23Checkpoint = checkpoint_path+tblID23

booksRecord_Schema = 'book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP'
customersRecord_Schema = 'customer_id STRING,first_name STRING, last_name STRING, gender STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp'
ordersRecord_Schema = 'order_id STRING, order_timestamp timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>'

SchemaImposedOnSource = 'key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp long'
tbl2FeedSchema = 'book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP'
tbl2Schema = 'book_id STRING, title STRING, author STRING, price DOUBLE, isActive BOOLEAN, start_date TIMESTAMP, end_date TIMESTAMP'
tbl3FeedSchema = 'customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp'
tbl3Schema = 'customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP'
tbl4Schema = 'order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>'
tbl5Schema = 'order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP'
tbl6Schema = ''

#current_catalog = str(spark.sql(f"SELECT current_catalog()").collect()[0][0])

# COMMAND ----------

# DBTITLE 1,helper 1
def clearAllButItem(path,ItemToRetain):
  for i in dbutils.fs.ls(path):
    if i.path != ItemToRetain:
        dbutils.fs.rm(i.path,recurse=True)
        print('Item deleted: ',i.path)

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

def delete_file_if_exists(file_path):
  if path_exists(file_path):
    dbutils.fs.rm(file_path)
    print('File deleted: ',file_path)
  else:
    print('File does not exists: ',file_path)

def delete_folder_if_exists(folder_path):
  if path_exists(folder_path):
    dbutils.fs.rm(folder_path,recurse=True)
    print('Folder deleted: ',folder_path)
  else:
    print('Folder does not exists: ',folder_path)

def drop_Database(dbname,dbStorage):
  delete_folder_if_exists(dbStorage)
  spark.sql(f"DROP DATABASE IF EXISTS {dbname} CASCADE")

def drop_Table(tbl,tblStorage,tblCheckpoint):
  delete_folder_if_exists(tblStorage)
  delete_folder_if_exists(tblCheckpoint)
  spark.sql(f"DROP TABLE IF EXISTS {tbl}")

def setPaths(tblID):
  vname_tblName = f'tbl_{tblID}'
  vname_tblStorage = f'tblStorage_{tblID}'
  vname_tblCheckpoint = f'tblCheckpoint_{tblID}'
  globals()[vname_tblName] = f'{dbname}.{tblID}'
  globals()[vname_tblStorage] = f'{dbStorage}{tblID}'
  globals()[vname_tblCheckpoint] = f'{checkpoint_path}{tblID}'
  print(
      vname_tblStorage,' = ',globals()[vname_tblStorage]
      ,'\n',vname_tblCheckpoint,' = ',globals()[vname_tblCheckpoint])

def create_Table(tbl,tblSchema):
  spark.sql(f"create table {tbl} ({tblSchema})")
  print('Table created with appropriate schema: ',tbl
        ,'\n',tblSchema)
  setPaths(tbl)

def cleanup_and_setup():
  drop_Database(dbname,dbStorage)
  setPaths(tbl11)
  setPaths(tbl12)
  setPaths(tbl13)
  setPaths(tbl21)
  setPaths(tbl22)
  setPaths(tbl23)
  setPaths(tbl24)
  setPaths(tbl25)
  drop_Table(tbl11,tbl11Storage,tbl11Checkpoint)
  drop_Table(tbl12,tbl12Storage,tbl12Checkpoint)
  drop_Table(tbl13,tbl13Storage,tbl13Checkpoint)
  drop_Table(tbl21,tbl21Storage,tbl21Checkpoint)
  drop_Table(tbl22,tbl22Storage,tbl22Checkpoint)
  drop_Table(tbl23,tbl23Storage,tbl23Checkpoint)
  drop_Table(tbl24,tbl24Storage,tbl24Checkpoint)
  clearAllButItem(kafka_raw,ItemToRetain)
  spark.sql(f"create database {dbname}")
  print('Database created: ',dbname)
  spark.sql(f"create table {tbl11} ({tbl2Schema})")
  print('Table created with appropriate schema: ',tbl11)
  setPaths(tbl11)
  spark.sql(f"create table {tbl3} ({tbl3Schema})")
  print('Table created with appropriate schema: ',tbl3)
  spark.sql(f"create table {tbl4} ({tbl4Schema})")
  print('Table created with appropriate schema: ',tbl4)
  spark.sql(f"create table {tbl5} ({tbl5Schema})")
  print('Table created with appropriate schema: ',tbl5)


# COMMAND ----------

# DBTITLE 1,helper 2
def get_index(dir):
    try:
        files = dbutils.fs.ls(dir)
        file = max(f.name for f in files if f.name.endswith('.json'))
        index = int(file.rsplit('.', maxsplit=1)[0])
    except:
        index = 0
    return index+1

def load_json_file(current_index,streaming_dir,raw_dir):
    latest_file = f"{str(current_index).zfill(2)}.json"
    source = f"{streaming_dir}/{latest_file}"
    target = f"{raw_dir}/{latest_file}"
    prefix = streaming_dir.split("/")[-1]
    if path_exists(source):
        print(f"Loading {prefix}-{latest_file} file to the bookstore dataset")
        dbutils.fs.cp(source, target)

def load_data(max,streaming_dir,raw_dir,all=False):
    index = get_index(raw_dir)
    if index > max:
        print("No more data to load\n")
    elif all == True:
        while index <= max:
            load_json_file(index, streaming_dir, raw_dir)
            index += 1
    else:
        load_json_file(index, streaming_dir, raw_dir)
        index += 1

def load_books_updates():
    streaming_dir = books_updates_streaming
    raw_dir = books_updates_raw
    load_data(5,streaming_dir, raw_dir)

def load_new_data(num_files = 1):
    streaming_dir = kafka_streaming
    raw_dir = kafka_raw
    for i in range(num_files):
        load_data(10, streaming_dir, raw_dir)

# COMMAND ----------


readQuery = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(SchemaImposedOnSource)
    .load(kafka_raw)
    .select(
        F.col("topic")
        ,F.col("key").cast("string")
        ,(F.col("timestamp")/1000).cast("timestamp").alias("create_ts")
        ,F.input_file_name().alias("Source_file")
        ,F.current_timestamp().alias("insert_ts")
        ,F.col("value").cast("string")
        )        
)

Topic='books'
# TopicSchema = globals[(Topic+'Record_Schema')]
# print(f"'{Topic}'")
t = globals().get(f"{Topic}Record_Schema")
print(f"'{t}'")

write_bronze_books = (
    readQuery
    .filter(F.col("topic")==Topic)
    .withColumn("v",F.from_json(F.col("value"),t))
    .select("key","create_ts","Source_file","insert_ts","v.*")
    # .writeStream
    # .option("checkpointLocation", tbl11Checkpoint)
    # .option("mergeSchema", True)
    # .trigger(availableNow=True)
    # .table(tbl11)
    )
# write_bronze_books.awaitTermination()

#display(readQuery)
display(write_bronze_books)
# display(spark.sql(f"select * from {tbl11}"))


# COMMAND ----------

# DBTITLE 1,Bronze layer
def writeData(readQuery,Topic,tbl,tblCheckpoint):
    TopicSchema = globals().get(f"{Topic}Record_Schema")    
    write_bronze_Topic = (
        readQuery
        .filter(F.col("topic")==Topic)
        .withColumn("v",F.from_json(F.col("value"),TopicSchema))
        .select("key","create_ts","Source_file","insert_ts","v.*")
        .writeStream
        .option("checkpointLocation",tblCheckpoint)
        .option("mergeSchema", True)
        .trigger(availableNow=True)
        # .table({tbl})
    )
    #write_bronze_Topic.awaitTermination()
    print(
    'Source\n',kafka_raw 
    ,'\nSchema Imposed On Source\n',TopicSchema
    ,'\nDest\n',tbl,'\n'
    )

def process_bronze():
    readQuery = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SchemaImposedOnSource)
        .load(kafka_raw)
        .select(
            F.col("topic")
            ,F.col("key").cast("string")
            ,(F.col("timestamp")/1000).cast("timestamp").alias("create_ts")
            ,F.input_file_name().alias("Source_file")
            ,F.current_timestamp().alias("insert_ts")
            ,F.col("value").cast("string")
            )
    )
    writeData(readQuery,'books',tbl11,tbl11Checkpoint)
    #writeData(readQuery,'customers',tbl12,tbl12Checkpoint)
    #writeData(readQuery,'orders',tbl13,tbl13Checkpoint)
    

# COMMAND ----------

process_bronze()

display(spark.sql(f"select * from {tbl11}"))
display(spark.sql(f"select * from {tbl12}"))
display(spark.sql(f"select * from {tbl13}"))

# COMMAND ----------

# DBTITLE 1,Bronze
def process_bronze():    
    query = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SchemaImposedOnSource)
        .load(kafka_raw)
        .withColumn("key", F.col("key").cast("string"))
        .withColumn("value", F.col("value").cast("string"))
        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
        .withColumn("Source_file", F.input_file_name())
        .writeStream
        .option("checkpointLocation", tbl1Checkpoint)
        .option("mergeSchema", True)
        .partitionBy("topic", "year_month")
        .trigger(availableNow=True)
        .table(tbl1))
    query.awaitTermination()
    print(
    'Source\n',kafka_raw 
    ,'\nSchema Imposed On Source\n',SchemaImposedOnSource
    ,'\nDest\n',tbl1,'\n',tbl1Storage,'\n',tbl1Checkpoint
    )

# type(query) --> pyspark.sql.streaming.query.StreamingQuery

# COMMAND ----------

# DBTITLE 1,Silver - Books
def scdType2_upsert(microBatchDF,microBatchID,tbl):
  microBatchDF.createOrReplaceTempView("batch")
  query = f"""
  MERGE INTO {tbl} AS dt
  USING
  (
      select null as merge_key,b.*
      from batch b
      union all
      select b.book_id as merge_key,b.*
      from batch b 
      inner join {tbl} t
      on t.book_id = b.book_id
      and t.isActive = 'Y' 
      and t.start_date <> b.update_date
  )AS st 
  ON dt.book_id = st.merge_key

  WHEN MATCHED AND dt.isActive = 'Y' AND dt.start_date <> st.update_date THEN 
  UPDATE SET
  dt.isActive = 'N',
  dt.end_date = st.update_date

  WHEN NOT MATCHED THEN
  INSERT (dt.book_id,dt.title,dt.author,dt.price,dt.isActive,dt.start_date,dt.end_date)
  VALUES (st.book_id,st.title,st.author,st.price,'Y',st.update_date,NULL);
  """
  microBatchDF.sparkSession.sql(query)

def process_silver_books():  
  query = (
     spark.readStream
    .table(tbl1)
    .filter("topic = 'books'")
    .select(F.from_json(F.col("value").cast("string"), tbl2FeedSchema).alias("v"))
    .select("v.*")
    .withColumnRenamed("updated","update_date")
    .writeStream
    .foreachBatch(lambda microBatchDF,microBatchID: scdType2_upsert(microBatchDF,microBatchID,tbl2))
    .option("checkpointLocation", tbl2Checkpoint)
    .trigger(availableNow=True)
    .start()
  )
  query.awaitTermination()
  print(
    'Source\n',tbl1
    ,'\nDest\n',tbl2,'\n',tbl2Storage,'\n',tbl2Checkpoint
    )

# COMMAND ----------

# DBTITLE 1,Silver - Customers
def scdType1_upsert(microBatchDF,microBatchID,tbl):
    window = F.window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    ranked_updatesDF = (
        microBatchDF
        .filter(F.col("row_status").isin(["insert", "update"]))
        .withColumn("rank", F.rank().over(window))
        .filter("rank == 1")
        .drop("rank")
       )
    ranked_updatesDF.createOrReplaceTempView("ranked_updates")

    query = f"""
    MERGE INTO {tbl3} dt
    USING ranked_updates st
    ON dt.customer_id = st.customer_id
    WHEN MATCHED AND dt.row_time < st.row_time 
    THEN UPDATE SET *
    WHEN NOT MATCHED
    THEN INSERT *
    """
    microBatchDF.sparkSession.sql(query)

def process_silver_customers():
    lookup_country = spark.read.json(lookup_country_path)
    query = (
        spark.readStream
        .table(tbl1)
        .filter("topic = 'customers'")
        .select(F.from_json(F.col("value").cast("string"), tbl3FeedSchema).alias("v"))
        .select("v.*")
        .join(F.broadcast(lookup_country),F.col("country_code") == F.col("code"),"inner")
        .writeStream
        .foreachBatch(lambda microBatchDF,microBatchID: scdType1_upsert(microBatchDF,microBatchID,tbl3))
        .option("checkpointLocation", tbl3Checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(
        'Source\n',tbl1
        ,'\nDest\n',tbl3,'\n',tbl3Storage,'\n',tbl3Checkpoint
        )

# window operations are only supported for static read DF and not for readStream DF. This problem can be resolved by using the window function in forEachBatch function, here we use microbatchDF and use batch syntax instead of streaming syntax. 
# In Merge operation, if source & target tables have different schemas, merging happens in accordance with dest table (target). 

# COMMAND ----------

# DBTITLE 1,Silver - Orders
def insert_only(microBatchDF,microbatchID,tbl):
  microBatchDF.createOrReplaceTempView("orders_microbatch")    
  query = f"""
  MERGE INTO {tbl} dt
  USING orders_microbatch st
  ON dt.order_id = st.order_id AND dt.order_timestamp = st.order_timestamp
  WHEN NOT MATCHED THEN INSERT *
  """
  microBatchDF.sparkSession.sql(query)

def process_silver_orders():    
    query = (
        spark.readStream.table(tbl1)
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), tbl4Schema).alias("v"))
        .select("v.*")
        .withWatermark("order_timestamp", "30 seconds")
        .dropDuplicates(["order_id", "order_timestamp"])
        .writeStream
        .foreachBatch(lambda microBatchDF,microBatchID: insert_only(microBatchDF,microBatchID,tbl4))
        .option("checkpointLocation", tbl4Checkpoint)
        .trigger(availableNow=True)
        .start()
        )
    query.awaitTermination()
    print(
        'Source\n',tbl1
        ,'\nDest\n',tbl4,'\n',tbl4Storage,'\n',tbl4Checkpoint
        )
#INSERT ONLY if not exists. implement duplicate check logic before inserting data into target table.
#dropDuplicates() can be used for both .read and .readstream objects.
# .foreachBatch does not require to explicitly pass parameters for MicroBatchDF & MicroBatchID. It only requires to pass a function which has that signature. Spark detects and pass those parameters automatically. 


# COMMAND ----------

# DBTITLE 1,Silver - Orders_Customers (Stream - Stream join)
def cdf_scdType1_upsert(microBatchDF,microbatchID,tbl):
    window = F.window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())    
    ranked_updates_df = (
        microBatchDF
        .filter(F.col("_change_type").isin(["insert", "update_postimage"]))
        .withColumn("rank", F.rank().over(window))
        .filter("rank = 1")
        .drop("rank", "_change_type", "_commit_version")
        .withColumnRenamed("_commit_timestamp", "processed_timestamp"))
    ranked_updates_df.createOrReplaceTempView("ranked_updates")
    
    query = f"""
    MERGE INTO {tbl5} dt
    USING ranked_updates st
    ON dt.order_id = st.order_id AND dt.customer_id = st.customer_id
    WHEN MATCHED AND dt.processed_timestamp < st.processed_timestamp
    THEN UPDATE SET *
    WHEN NOT MATCHED
    THEN INSERT *
    """
    microBatchDF.sparkSession.sql(query)

def process_silver_orders_customers():    
    query = (
        spark.readStream.table(tbl4)
        .join((
            spark.readStream
            .option("readChangeData", True)
            .option("startingVersion", 2)
            .table(tbl3)
            )
            ,["customer_id"]
            ,"inner")
        .writeStream
        .foreachBatch(lambda microBatchDF,microBatchID: cdf_scdType1_upsert(microBatchDF,microBatchID,tbl5))
        .option("checkpointLocation", tbl5Checkpoint)
        .trigger(availableNow=True)
        .start()
        )
    query.awaitTermination()    
    print(
        'Source\n',tbl3,'\n',tbl4 
        ,'\nDest\n',tbl5,'\n',tbl5Storage,'\n',tbl5Checkpoint
        )

# COMMAND ----------

# DBTITLE 1,Silver - Orders_Books (Stream - Static join)


# COMMAND ----------

# MAGIC %md
# MAGIC ##EXIT (LAST CELL)

# COMMAND ----------

# DBTITLE 1,exit
dbutils.notebook.exit("Notebook execution completed successfully.")

# COMMAND ----------

# DBTITLE 1,Original
def process_bronze():
    query = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .schema(schema)
             .load(f"{dataset_bookstore}/kafka-raw")
                            .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                            .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                            .withColumn("source_file",F.input_file_name())
                      .writeStream
                          .option("checkpointLocation", 'dbfs:/mnt/demo_pro/checkpoints/bronze')
                          .option("mergeSchema", True)
                          .outputMode ('Append')
                          .partitionBy("topic", "year_month")
                          .trigger(availableNow=True)
                          .table('bookstore_eng_pro.bronze'))

        query.awaitTermination()

# COMMAND ----------


def scdType2_upsert_wrapper(destTable,destTableCheckpoint):
  def inner_func(microBatchDF,microBatchID):
    type2_upsert(destTable,destTableCheckpoint,microBatchDF,microBatchID)
    return inner_func

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace table batch as
# MAGIC SELECT 
# MAGIC     v.book_id, 
# MAGIC     v.title, 
# MAGIC     v.author, 
# MAGIC     v.price, 
# MAGIC     v.updated 
# MAGIC FROM (
# MAGIC     SELECT from_json(value, schema_of_json('{"book_id":"B15","title":"Inside the Java Virtual Machine","author":"Bill Venners","price":51.0,"updated":"2021-11-18 17:12:42.335"}')) AS v
# MAGIC     FROM bookstore_eng_pro.bronze
# MAGIC     WHERE topic = 'books'
# MAGIC     AND Source_file = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw/books-updates/01.json'
# MAGIC ) AS parsed_data;
# MAGIC
# MAGIC create or replace temp view vw_batch as
# MAGIC select * from
# MAGIC (select * ,updated as update_date
# MAGIC ,max(updated) over (partition by book_id) as max_update_date
# MAGIC from batch)t
# MAGIC where updated = t.max_update_date;
# MAGIC
# MAGIC select * from vw_batch;
# MAGIC
# MAGIC /*
# MAGIC (
# MAGIC   recID INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   book_id VARCHAR,
# MAGIC   title VARCHAR,
# MAGIC   author VARCHAR,
# MAGIC   price float,
# MAGIC   update_date timestamp
# MAGIC )
# MAGIC */
# MAGIC
# MAGIC %sql
# MAGIC --select * from bookstore_eng_pro.silver_books;
# MAGIC truncate table bookstore_eng_pro.silver_books;

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"])))

display(customers_df)

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window))
                          .filter("rank == 1")
                          .drop("rank"))
display(ranked_df)

# This will throw an exception because non-time-based window operations are not supported on streaming DataFrames.
ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

display(ranked_df)

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

%sql
CREATE TABLE IF NOT EXISTS customers_silver
(customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(df_country_lookup)

query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

