# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Count() is a Transformation or Action? --> depends on usage:
# MAGIC - df.count() --> action (used standalone)
# MAGIC - df.select(count('name')) --> select() is a narrow transformation, count() is an expression.
# MAGIC - df.groupBy('name').count() --> groupBy()  is a wide transformation, count() is an aggr function.
# MAGIC - 1 is Action, 2 & 3 are Transformations.
# MAGIC - df.count() OR df.select(count('*')) OR df.select(count('1'))  --> all 3 give same results i.e. count all records in df. However, df.select(count('name')) counts only those records where 'name' <> NULL.
# MAGIC
# MAGIC ### Distinct
# MAGIC - **df.distinct()** : identify distinct records in df. If you pass one or more columns inside, it will return distinct combination of those columns.
# MAGIC - **df.countDistinct()**: pass a subset of columns for which uniqueness needs to be checked; count is calculated on distinct combinations available for those. 
# MAGIC - **df.select('colname').distinct()** : includes NULL (if exists) alongwith other distinct values.
# MAGIC
# MAGIC ### Sum, Min, Max, Avg
# MAGIC - Except count(), all other aggregate functions are **always transformation**. 
# MAGIC - results are **impacted due to NULL values** **only for count() and avg()** functions (avg is based on count). Others do not care about NULL values. 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window as w

# COMMAND ----------

employee_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]
employee_schema = ['id','name','age','salary','country','department']

employee = spark.createDataFrame(data=employee_data,schema=employee_schema).sort(col('id'))
employee.display()

# COMMAND ----------

#below are actions (Specially available only for count() function)
employee.count() #10 --> counts all records
employee.distinct().count() #9 --> counts distinct records
employee.select('name').distinct().count() #8 count of distinct names (distinct() considers NULL value also)

#below are transformations
employee.select(count(col('*'))).show() #same as df.count()
employee.select(count(col('id'))).show() #counts only those records where id is not NULL
employee.select(count(lit('1'))).show() #creates literal column with constant value '1' and counts records. 
employee.groupBy('name').count().show() #accepts NULLs
employee.distinct().show() #9 works similar to employee.distinct().count()

# get count of distinct names (does not consider NULL value)
employee.select(countDistinct('name')).show() #7
employee.select(countDistinct()).show() #error for '1' or blank
employee.select(countDistinct('*')).show() #5
employee.select('name').distinct().select(count('name')).show() #7 this is equivalent of above operation.

employee.select('name').distinct().count() #8

employee.select('name').count() #10
employee.select('name').distinct().show() #8


# COMMAND ----------

employee.select(
    min('salary'),max('salary')
    ,sum('salary').alias('sum_salary')
    ,count('*').alias('count_*')
    ,count('salary').alias('count_salary')
    ,(sum('salary')/count('*')).cast('int').alias('sum_salary/count_*')
    ,(sum('salary')/count('salary')).cast('int').alias('sum_salary/count_salary')
    ,avg('salary').cast('int').alias('avg_salary')
    )\
        .show()
#avg('salary)function calculates based on only those records where salary is not NULL. avg('salary) not match with:
    # sum('salary')/count('*') because count('*') counts NULL records also.
    # sum('salary')/count('id') because count('id') has got 1 record more than count('salary') --> check dataset; salary is NULL for id=4.

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupBy & agg

# COMMAND ----------

employee.select(sum('salary')).display() #total salary for all employees together
employee\
    .groupBy('country','department')\
    .agg(sum('salary'),count('salary'))\
    .sort('country','department')\
    .display() #total salary by (country, department)

employee.groupBy('name').count().show()

employee.groupBy('name').agg(count('name')).show()

employee.groupBy('name').agg(count('*')).show()

employee.agg(count('name')).show()
employee.select(count('name')).show()

employee.select(count()).show()

type(employee.count())
d1 = employee.groupBy()
type(d1)

#sum of all numeric columns
employee.groupBy().sum().show()

help(employee.agg)

#Spark SQL
employee.createOrReplaceTempView('vw_employee')
spark.sql('''
          select country,department,sum(salary),count(salary)
          from vw_employee
          group by country,department
          order by country,department
          ''')\
        .display()

