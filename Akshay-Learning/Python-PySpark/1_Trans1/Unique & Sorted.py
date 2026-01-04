# Databricks notebook source
# MAGIC %md
# MAGIC ### .distinct()
# MAGIC - returns unique rows. uniqueness is decided based on which columns are selected. 
# MAGIC - If specific columns need to be passed for distinct, do it using .select(). If there is no .select(), it  will check for uniqueness on full records. 
# MAGIC
# MAGIC ### .dropDuplicates()
# MAGIC - dropDuplicates() is used for PySpark dataframe, drop_duplicates() is used for Pandas dataframe. For the convinience of Python developers, PySpark API has created an alias name for dropDuplicates() with same name from Pandas method as "drop_duplicates()". This alias internally calls the PySpark method dropDuplicates() itself, and has nothing to do with drop_duplicates() method in Pandas. 
# MAGIC - unlike distinct(), allows to pass subset of columns for which duplicates need to be checked. If nothing is passed, by default it will check for uniqueness on full records. 
# MAGIC
# MAGIC ### Sort()
# MAGIC - pass columns using col('colname').asc() or .desc()
# MAGIC - default is .asc()
# MAGIC - sort and orderby work exactly same.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,data
employee_data=[
    (10 ,'Anil',50000,'Thane', 18),
    (11 ,'Vikas',75000,'Pune',  16),
    (12 ,'Nisha',40000,'Mumbai',  18),
    (13 ,'Nidhi',60000,'Delhi',  17),
    (13 ,'Nidhi',60000,'Chennai',  17),
    (14 ,'Priya',80000,'Hyderabad',  28),
    (14 ,'Priya',90000,'Hyderabad',  20),
    (15 ,'Mohit',45000,'Kochi',  18),
    (15 ,'Mohit',50000,'Hyderabad',  18),
    (18 ,'Sam',65000,'Delhi',   17),  
    (18 ,'Sam',65000,'Delhi',   17),
    (16 ,'Rajesh',90000,'Chennai', 10),
    (17 ,'Raman',55000,'Chennai', 16)
     ]

employee_schema = ['id','Name','Salary','Address','mngr_id']
employee = spark.createDataFrame(data=employee_data,schema=employee_schema)

# COMMAND ----------

# DBTITLE 1,distinct
employee.display() #13 records
#employee.distinct().display() #12 records
#employee.select('id','Name').distinct().display() #9 records


# COMMAND ----------

# DBTITLE 1,drop duplicates
employee.dropDuplicates().display() #12 records - checks for uniqueness on full record and drops duplicates accordingly. works same as distinct(). 

employee\
    .select('Name','Address','mngr_id','Salary')\
    .dropDuplicates(['Name','Address'])\
    .display() 

"""
checks for uniqueness on (['Name','Address']) and returns full rows unlike .distinct(). POssible Scenarios (for 2 or more records):
1a. Name & Address both are same, all other fields are also same --> keeps 1 record, drops all other records.
1b. Name & Address both are same, 1 other field is different --> keeps 1st occurence record, drops all consecutive records.
2. Name & Address both are different --> keep all records.
3. Name is same, Address is different --> keep all records.

Advantage of using .dropDuplicates(['Name','Address']) over .select('id','Name').distinct() --> Spark selects entire dataframe ireespective of what is passed inside dropDuplicates()

"""


# COMMAND ----------

# DBTITLE 1,sort
employee.sort(col('id').desc(),col('Address')).display()

# COMMAND ----------

# DBTITLE 1,Leetcode
my_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]
my_schema = ['id','name','reference_id']
dataframe = spark.createDataFrame(data=my_data,schema=my_schema)

a = dataframe.alias('a')
b = dataframe.alias('b')

#those who are not referenced by '2'
output = a\
    .filter((col('a.reference_id')!=2)|(col('a.reference_id').isNull()))\
    .display()

#display reference names for each employee
output2 = a\
    .join(b,col('a.reference_id')==col('b.id'),'left')\
    .withColumn('reference_name',col('b.name'))\
    .withColumn('reference_name',
                when (col('reference_name').isNull(),lit('No reference'))
                .otherwise(col('reference_name')))\
    .select(col('a.id'),col('a.name'),col('a.reference_id'),col('reference_name'))\
    .sort(col('a.id'))\
    .display()
