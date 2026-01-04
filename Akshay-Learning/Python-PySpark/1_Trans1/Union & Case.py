# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Below items from source tables impact union results:
# MAGIC - number of columns
# MAGIC - names of columns 
# MAGIC - order of columns
# MAGIC
# MAGIC ### Union v/s Union All ()
# MAGIC - Pyspark Dataframe --> both results same.
# MAGIC - Spark SQL --> returns distinct rows from union results.
# MAGIC - union (or union all) work fine as long as **number** of columns are same in both source tables. Even if names or order of columns is different, it will still give results without throwing error (results are not in a good shape though). It does not bother for column names or column order.
# MAGIC
# MAGIC ### UnionByName
# MAGIC - works fine as long as **number and names** of columns are same in both source tables. Even if order of columns is different, it will match the column names and give results.
# MAGIC - This is useful when order of column is different in both tables, but number of columns is same.
# MAGIC
# MAGIC ### Possible scenarios
# MAGIC - Number, Name, Order are same --> union
# MAGIC - Number and Name are same, Order is different --> unionByName
# MAGIC - Number and Order is same, Name is different --> union
# MAGIC - Number is different --> manually select required columns --> union/ unionByName (based on Order)
# MAGIC
# MAGIC ### Difference between 2 datasets
# MAGIC - subtract() --> (A-B) but drop duplicates.
# MAGIC - exceptAll() --> same as subtract(), without dropping duplicates. 

# COMMAND ----------

employee_data=[
    (10 ,'Anil',50000, 18),
    (11 ,'Vikas',75000,  16),
    (12 ,'Nisha',40000,  18),
    (13 ,'Nidhi',60000,  17),
    (14 ,'Priya',80000,  18),
    (15 ,'Mohit',45000,  18),
    (16 ,'Rajesh',90000, 10),
    (17 ,'Raman',55000, 16),
    (18 ,'Sam',65000,   17)
]
employee_data_new=[
    (19 ,'Sohan',50000, 18),
    (20 ,'Sima',75000,  17)
    ]
employee_data_new_duplicated=[
    (19 ,'Sohan',50000, 18),
    (20 ,'Sima',75000,  17),
    (20 ,'Sima',75000,  17),
    (20 ,'Sima',65000,  17)
    ]
employee_schema = ['id','Name','Salary','mngr_id']

wrongOrder_data=[
    (19 ,50000, 18,'Sohan'),
    (20 ,75000, 17,'Sima')
    ]
wrongOrder_schema = ['id','Salary','mngr_id','Name']
wrongOrderName_schema = ['id','Salary','Manager_id','Name']

wrongOrderNameList_data=[
    (19 ,50000, 18,'Sohan',10),
    (20 ,75000, 17,'Sima',20)
    ]
wrongOrderNameList_schema = ['id','Salary','mngr_id','Name','Bonus']

def fn_createDataFrame(data,schema):
    return spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------


employee = fn_createDataFrame(employee_data,employee_schema)
employee_new = fn_createDataFrame(employee_data_new,employee_schema)
employee_new_duplicated = fn_createDataFrame(employee_data_new_duplicated,employee_schema)

wrongOrder = fn_createDataFrame(wrongOrder_data,wrongOrder_schema)
wrongOrderName = fn_createDataFrame(wrongOrder_data,wrongOrderName_schema)
wrongOrderNameList = fn_createDataFrame(wrongOrderNameList_data,wrongOrderNameList_schema)


# COMMAND ----------

# DBTITLE 1,Union
employee.union(employee_new).display() #9+2=11 records

#union and unionAll give same results in PySpark Dataframe irrespective of duplicates.
#gives 13 records for both operations below.
employee.union(employee_new_duplicated).display()
employee.unionAll(employee_new_duplicated).display()

#union and unionAll give different results in Spark SQL if there exists duplicates.
employee.createOrReplaceTempView('vw_employee')
employee_new_duplicated.createOrReplaceTempView('vw_employee_new_duplicated')

spark.sql("""
          select * from 
          (
           select * from vw_employee
           union
           select * from vw_employee_new_duplicated
          )""").display() #12 records

spark.sql("""
          select * from 
          (
           select * from vw_employee
           union all
           select * from vw_employee_new_duplicated
          )""").display() #13 records

#perform union/ union all operations with table itself; it behaves same as above.  
spark.sql("""
          select * from 
          (
           select * from vw_employee_new_duplicated
           union
           select * from vw_employee_new_duplicated
          )""").display() #3 records

spark.sql("""
          select * from 
          (
           select * from vw_employee_new_duplicated
           union all
           select * from vw_employee_new_duplicated
          )""").display() #8 records

# COMMAND ----------

employee.union(wrongOrder).display() #performs union but not checks for column order
employee.unionByName(wrongOrder).display() #performs union as well as checks for column order

employee.union(wrongOrderName).display() #performs union but ignores column names
employee.unionByName(wrongOrderName).display() #throws error (column order can be resolved using unionByName, but column name is different for manager_id v/s mngr_id)

employee.union(wrongOrderNameList).display() #throws error (column list)
employee.unionByName(wrongOrderNameList).display() #throws error (column list)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Case statement
# MAGIC - Pyspark Dataframe --> **when (1st)** (condition, action) --- **.when (consecutive)** (condition, action) --- **.otherwise** (action)
# MAGIC     - Spark don't find matching when stmt, and .otherwise stmt is also missing --> puts NULL value in column. 
# MAGIC - Spark SQL --> case when condition1 then action1 when condition2 then action2 else end action3 as column_alias.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,PySpark
employee2 = employee
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("mngr_id", IntegerType(), True)
])
employee2 = employee2.union(fn_createDataFrame([Row(id=10 ,Name='Rohit',Salary=None, mngr_id=18)],schema))
employee2.show()

employee2\
    .withColumn('Cadre',
                when (col('Salary').isNull(), lit('Salary not available'))
                .when (col('Salary')<50000,'Junior')
                .when ((col('Salary')>=50000) & (col('Salary')<80000),lit('Senior'))
                .otherwise ('Cadre not available')
                )\
    .display()

# COMMAND ----------

# DBTITLE 1,Spark SQL
employee2.createOrReplaceTempView('vw_employee2')

spark.sql("""
            select  *
                    ,case 
                    when Salary is null then 'Salary not available'
                    when Salary<50000 then 'Junior'
                    when Salary>=50000 and Salary<80000 then 'Senior'
                    else 'Cadre not available'
                    end as Cadre
            from    vw_employee2
          """)\
    .display()
