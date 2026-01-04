# Databricks notebook source
# DBTITLE 1,Function call
# MAGIC %run ../Includes/Function_call

# COMMAND ----------

# DBTITLE 1,Set variables
setVars()

# COMMAND ----------

# DBTITLE 1,Initial
displayHTML("""
            <h5> Bronze and Silver at intial stage (12 records) </h5> 
            """)
display(spark.sql(f"select * from {tbl_bronze_books} order by key"))
display(spark.sql(f"select * from {tbl_silver_books} order by book_id,start_date desc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##CDC feed #1 (No changes in data)

# COMMAND ----------

# DBTITLE 1,CDC Feed #1 (Book updates)

displayHTML("""
            <h5> Import CDC Feed #1 ; process Bronze & Silver tables </h5> 
            """)
load_books_updates()
process_bronze()
process_silver_books()

# COMMAND ----------

# DBTITLE 1,Final
displayHTML("""
            <h5> Bronze after processing CDC Feed #1 (12 at begin + 5 new from source = 17 records) </h5> 
            <h5> Silver after processing CDC Feed #1 (12 at begin + 5 new from bronze = 17 records) </h5>
            """)
display(spark.sql(f"select * from {tbl_bronze_books} order by insert_ts desc,key"))                      
display(spark.sql(f"select * from {tbl_silver_books} order by book_id,start_date desc"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###CDC feed #2 (Changed data)

# COMMAND ----------

# DBTITLE 1,CDC Feed #2 (Book updates)
displayHTML("""
            <h5> Import CDC Feed #2 </h5> 
            """)
load_books_updates()
process_bronze()
process_silver_books()

# COMMAND ----------

# DBTITLE 1,Final
displayHTML("""
            <h5> Bronze after processing CDC Feed #2 (17 at begin + 5 new from source = 22 records) </h5> 
            <h5> Silver after processing CDC Feed #2 (17 at begin + 5 new from bronze = 22 records) </h5> 
            """)
display(spark.sql(f"select * from {tbl_bronze_books} order by key,create_ts desc"))                      
display(spark.sql(f"select * from {tbl_silver_books} order by book_id,start_date desc"))


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table bookstore_eng_pro.silver_current_books as
# MAGIC select * from bookstore_eng_pro.silver_books
# MAGIC where isActive='true';
# MAGIC
# MAGIC select * from bookstore_eng_pro.silver_current_books;
# MAGIC drop table bookstore_eng_pro.silver_current_books;
