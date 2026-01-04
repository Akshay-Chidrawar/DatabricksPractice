# Databricks notebook source
# MAGIC %md
# MAGIC ### Data can be transformed using:
# MAGIC - PySpark Dataframe API
# MAGIC - Spark SQL

# COMMAND ----------

# DBTITLE 1,Hard coded dataframe creation
my_data=[(1,20),(3,40)]
my_schema=['id','score']

my_df = spark.createDataFrame(data=my_data, schema=my_schema)
my_df.printSchema()
my_df.show()

data = [(1,2,3)]
df = spark.createDataFrame(data).toDF('col1','col2','col3').show()


# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql.functions import *
from pyspark.sql.types import *
#maximum methods are available in these 2 libraries.

file_path = 'dbfs:/FileStore/ManishKumar/employee_data.csv'

# COMMAND ----------

employee_df2 = spark.read\
    .options(header='true', mode='PERMISSIVE',inferschema='True')\
    .format('csv')\
    .load(file_path)

employee_df2.printSchema()
employee_df2.columns

# COMMAND ----------

# DBTITLE 1,Add new row
from pyspark.sql import Row

new_row = Row(10,'Ramesh',45,30000,'Mumbai','Suresh','')
new_row_df = spark.createDataFrame(data=[new_row],schema=employee_df2.schema) #data must be an iterable of rows, not a single Row.

employee_df2=employee_df2.union(new_row_df)

employee_df2.show()

# COMMAND ----------

# DBTITLE 1,impose schema
employee_schema = StructType([
  StructField('id', IntegerType(), True)
  ,StructField('name', StringType(), True)
  ,StructField('age', IntegerType(), True)
  ,StructField('salary', IntegerType(), True)
  ,StructField('address', StringType(), True)
  ,StructField('nominee', StringType(), True)
])

employee_df = spark.read\
    .options(header='true', mode='PERMISSIVE')\
    .schema(employee_schema)\
    .format('csv')\
    .load(file_path)

employee_df.show()
employee_df.printSchema()
employee_df.columns

# COMMAND ----------

# DBTITLE 1,Select operations
employee_df.select('nam'+'e').show() # the + operator here is used to evaluate the existing column name.
employee_df.select(col('id')+5).show() # the + operator here is used to derive a new column with value +5. No column name mentioned. 
employee_df.select(expr('id+5'),expr('id as emp_id'),expr('concat(id," | ",name) as emp_id_name')).show()
#expr is used for SQL expressions inside Python syntax
employee_df.select(employee_df['salary'],employee_df.address).show() # this way of referencing columns is helpful while working on joins. 

# COMMAND ----------

# DBTITLE 1,alias, lit, withColumn & Rename, filter, drop
employee_df.select('*',col('id').alias('emp_id'),'age').show() # .alias() will rename column. if nothing is passed inside select, it does not give error, but wont show proper table as well. 
employee_df.drop('salary',col('nominee')).show() #drop column

employee_df.select('*',lit('last name unknown (LNU)').alias('Last_name')).show(truncate=False)
#literal is used to set a constant value (initialize/ default) for all rows, while creating a new column. if you do not pass alias(), the same constant value is used as the column name as well. 

employee_df\
    .select('salary',col('age'))\
    .withColumn('age after 5 years',col('age')+5)\
    .withColumn('Last_name',lit('last name unknown (LNU)'))\
    .withColumnRenamed('id','emp_id')\
    .show(truncate=False)
#withColumn returns the existing dataframe with the new column attached at end. If there is an existing column with same name, withColumn overwrites it, else creates a new column. 
#withColumnRenamed is same as .alias()

from pyspark.sql.functions import *
from pyspark.sql.types import *

employee_df\
    .withColumn('id',col('id').cast(StringType()))\
    .printSchema()
#this drops existing column after creating a new column with same name, but data type changed from int to string.

employee_df.filter((col('salary')>100000) & (col('age')>18)).show() #you can also use .where() - both are same. Do not use AND, OR, NOT keywords. Instead, use &, |, ~ bitwise operators.



# COMMAND ----------

# DBTITLE 1,Spark SQL

employee_df.createOrReplaceTempView('vw_employee_df')

spark.sql("""
          select  --*,
                  id 
                  ,name as First_name 
                  ,'LNU' as Last_name
                  ,cast(salary as double) as salary
                  ,concat(name,' ',Last_name) as full_name
                  ,cast(id as string) as id
          from    vw_employee_df
          where   salary>100000 and age>18
          """)\
        .show()
