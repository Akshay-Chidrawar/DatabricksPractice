# Databricks notebook source
# MAGIC %md
# MAGIC ### Count() 
# MAGIC
# MAGIC is the only aggregate function which behaves as a transformation as well as an action; based on where it is used:
# MAGIC   - df.select(count(col('id'))) --> transformation (.select() is a transformation, count() is a col expression)
# MAGIC   - df.groupBy(col('colName')).count() --> transformation
# MAGIC   - df.count() --> action (this syntax does not accept any argument)
# MAGIC
# MAGIC gives different values based on how it is used: Let us say there are 6 records in a table, with one record where id is NULL. When below column expressions are used in a .select() statement:
# MAGIC   - count(col('id')) --> 5 (counts only those records where ID is not NULL).
# MAGIC   - count(col('*')) --> 6. (counts all records without any filter; equivalent as df.count())
# MAGIC
# MAGIC countDistinct()
# MAGIC   - pass a subset of columns for which uniqueness needs to be checked and count is calculated on those distinct combinations. 
# MAGIC
# MAGIC ### Sum, Min, Max, Avg
# MAGIC - only count() function behaves as both transformation & action. Other aggregate functions are always transformation. 
# MAGIC - only count() function results are impacted due to NULL values. Others do not care about NULL values. 
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

#below are transformations 
employee.select(count(col('*'))).show() #same as df.count()
employee.select(count(col('id'))).show() #counts only those records where id is not NULL
employee.select(count(lit('1'))).show() #creates a literal column with constant value '1' and then counts records with not NULL values in this column. Since all are '1', all records are eligible. Purpose- when we dont want to reference a specific column to take a row count. works same as count(*). 

# get count of distinct names (does not consider NULL value)
employee.select(countDistinct('name')).show() #7
employee.select('name').distinct().select(count('name')).show() #7 this is equivalent of above operation.

#below are actions (Specially available only for count() function)

employee.count() #10
employee.distinct().count() #9
employee.select('name').distinct().count() #8 count of distinct names (distinct() considers NULL value also)


# COMMAND ----------

employee.select(sum('salary'),min('salary'),max('salary')
                ,avg('salary').cast('int')
                ,(sum('salary')/count('salary')).cast('int'))\
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
    .display() #total salary country & department wise

#Spark SQL
employee.createOrReplaceTempView('vw_employee')
spark.sql('''
          select country,department,sum(salary),count(salary)
          from vw_employee
          group by country,department
          order by country,department
          ''')\
        .display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Window functions
# MAGIC - window function --> partitionBy is mandatory, orderBy is not. 
# MAGIC
# MAGIC ### orderBy clause: 
# MAGIC - required whenever order of elements of the window are considered for calcuation. Eg - **rank** functions, **cumulative** scenarios, **lead & lag** functions. orderBy is not required for regular sum(), count(), etc functions. 
# MAGIC - When we use orderBy, by default the window slides between (unboundedPreceding, current row).
# MAGIC
# MAGIC - cumulative scenarios calculate values based on ranking of the records. Records need to be sorted on some order (eg- salary). In case 2 or more records have same ranking, cumulative sum() will calculate for all those records together and assign that value common to all those records. Refer below example. 
# MAGIC - rank functions --> rank() assigns, dense_rank()
# MAGIC - row_number() --> gives row number (no scope for ranking or duplicate rows)

# COMMAND ----------

# DBTITLE 1,Data
employee_data =[
    (1,'manish',50000,'IT','India'),
    (2,'vikash',60000,'sales','US'),
    (3,'raushan',45000,'marketing','India'),
    (4,'mukesh',80000,'IT','US'),
    (5,'pritam',90000,'sales','India'),
    (6,'nikita',45000,'marketing','US'),
    (7,'ragini',55000,'marketing','India'),
    (8,'rakesh',65000,'IT','US'),
    (9,'aditya',65000,'IT','India'),
    (10,'rahul',50000,'marketing','US')
    ]
employee_schema = ['id','name','salary','department','country']

employee = spark.createDataFrame(data=employee_data,schema=employee_schema).sort(col('id'))
employee.display()

# COMMAND ----------

# DBTITLE 1,Q1
window_spec1 = w.partitionBy('department') 
#spark is not sorting salaries; for each department, sum() function adds up all salaries at once.

window_spec2 = w.partitionBy('department').orderBy(col('salary').desc())
#for each department, orderBy clause forces spark to sort salaries, before sum() function calculates running sum;

#if we use .groupBy and .agg, it shrinks the dataframe. 
#use window functions to avoid such shrinking.
result = employee\
    .select('id','name','department','salary')\
    .withColumn('row_number',row_number().over(window_spec2))\
    .withColumn('rank',rank().over(window_spec2))\
    .withColumn('dense_rank',dense_rank().over(window_spec2))\
    .withColumn('department_budget',sum('salary').over(window_spec1))\
    .withColumn('cumulative_salary',sum('salary').over(window_spec2))

result.display()

# COMMAND ----------

# DBTITLE 1,Q2
#top 2 performers in each department

result\
    .filter(col('dense_rank')<=2)\
    .select('id','name','department','salary','dense_rank')\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lead and Lag syntax:
# MAGIC - (col_name,jump,value_for_first_record)
# MAGIC - value_for_first_record = NULL (by default)

# COMMAND ----------

# DBTITLE 1,Data
sales_data = [
(1,'iphone','01-01-2023',1500000),
(2,'samsung','01-01-2023',1100000),
(3,'oneplus','01-01-2023',1100000),
(1,'iphone','01-02-2023',1300000),
(2,'samsung','01-02-2023',1120000),
(3,'oneplus','01-02-2023',1120000),
(1,'iphone','01-03-2023',1600000),
(2,'samsung','01-03-2023',1080000),
(3,'oneplus','01-03-2023',1160000),
(1,'iphone','01-04-2023',1700000),
(2,'samsung','01-04-2023',1800000),
(3,'oneplus','01-04-2023',1170000),
(1,'iphone','01-05-2023',1200000),
(2,'samsung','01-05-2023',980000),
(3,'oneplus','01-05-2023',1175000),
(1,'iphone','01-06-2023',1100000),
(2,'samsung','01-06-2023',1100000),
(3,'oneplus','01-06-2023',1200000)
]
sales_schema = ['id','product','sale_date','units']

sales = spark.createDataFrame(data=sales_data,schema=sales_schema).sort(col('id').asc(),col('sale_date').asc())
sales.display()

# COMMAND ----------

# DBTITLE 1,Solution
# percentage of sales each month w.r.t. total sales.
# percentage of sales rise/ drop compared to previous month.

window_spec1 = w.partitionBy(col('product'))
window_spec2 = w.partitionBy(col('product')).orderBy(col('sale_date'))

result = sales\
    .withColumn('total_sales',sum(col('units')).over(window_spec1))\
    .withColumn('perc_sales',
                concat(
                    ((col('units')/col('total_sales'))*100).cast(DecimalType(precision=5,scale=2))
                    ,lit(' %')
                    )
                )\
    .withColumn('prevMonth_sales',lag(col('units'),1,'0').over(window_spec2))\
    .withColumn('Perc_change',
                concat(
                    (((col('units')-col('prevMonth_sales'))/col('prevMonth_sales'))*100).cast(DecimalType(precision=5,scale=2))
                    ,lit(' %')
                    )
                )\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row between
# MAGIC - when we use orderBy() in window definition, by default the window slides between (unboundedPreceding, current row). We can change this limit using rowsBetween() function:
# MAGIC     - unboundedPreceding --> full scope for lower side of window.
# MAGIC     - unboundedFollowing --> full scope for upper side of window.
# MAGIC     - current_row --> indicates current row of window; denoted by 0. 
# MAGIC     - put hard coded values like ...,-2,-1,0, 1,2,...

# COMMAND ----------

# DBTITLE 1,Q1 data
sales_data = [
(2,'samsung','01-01-1995',11000),
(1,'iphone','01-02-2023',1300000),
(2,'samsung','01-02-2023',1120000),
(3,'oneplus','01-02-2023',1120000),
(1,'iphone','01-03-2023',1600000),
(2,'samsung','01-03-2023',1080000),
(3,'oneplus','01-03-2023',1160000),
(1,'iphone','01-01-2006',15000),
(1,'iphone','01-04-2023',1700000),
(2,'samsung','01-04-2023',1800000),
(3,'oneplus','01-04-2023',1170000),
(1,'iphone','01-05-2023',1200000),
(2,'samsung','01-05-2023',980000),
(3,'oneplus','01-05-2023',1175000),
(1,'iphone','01-06-2023',1700000),
(3,'oneplus','01-01-2010',23000),
(2,'samsung','01-06-2023',1100000),
(3,'oneplus','01-06-2023',1200000)
]
sales_schema=['product_id','product_name','sales_date','sales']

sales = spark.createDataFrame(data=sales_data,schema=sales_schema).sort(col('product_id').asc(),col('sales_date').asc())

sales.display()

# COMMAND ----------

# DBTITLE 1,Q1 solution
#calculate difference between latest and oldest sales of each product. 

# However, in below example, we need to slide over an entire window to access oldest and latest data.
window_spec1 = w.partitionBy('product_id').orderBy('sales_date').rowsBetween(w.unboundedPreceding,w.unboundedFollowing)

result = sales\
    .withColumn('oldest_sales',first(col('sales')).over(window_spec1))\
    .withColumn('latest_sales',last(col('sales')).over(window_spec1))\
    .withColumn('latest_oldest_sales_diff',(col('latest_sales')-col('oldest_sales')))\
    .select('product_id','product_name','latest_oldest_sales_diff')\
    .distinct()\
    .display()

# COMMAND ----------

# DBTITLE 1,Q1a solution
#calculate rolling sales for "n" months for a given dimension. 

n=-3
dim_name = 'product_name'

import builtins
py_abs = builtins.abs #abs method is available in both Python and PySpark. However, we need to use Python's abs() method, since PySpark's abs() method works on columns, but not on variables. 

col_name = 'Roll '+to_str(py_abs(n))+' months_Sales'

# when we use orderBy(), by default rowsBetween() slides over a window of (unboundedPreceding, current row). However, in below example, we need to slide over a limited window to access last 3 months and latest data.
window_spec1 = w.partitionBy(dim_name).orderBy('sales_date').rowsBetween(n+1,0)
window_spec2 = w.partitionBy(dim_name).orderBy('sales_date')

result = sales\
    .withColumn(col_name,sum(col('sales')).over(window_spec1))\
    .withColumn('row_number',row_number().over(window_spec2))\
    .withColumn(col_name+'_filtered',
                when ((col('row_number')<=py_abs(n)-1),None)
                .otherwise(col(col_name))
                )\
    .select(dim_name,'sales_date','sales',col_name,col_name+'_filtered')\
    .distinct()\
    .display()

# COMMAND ----------

# DBTITLE 1,Q2 data
employee_data = [
    (1,'manish','11-07-2023','10:20'),
    (1,'manish','11-07-2023','11:20'),
    (2,'rajesh','11-07-2023','11:20'),
    (1,'manish','11-07-2023','11:50'),
    (2,'rajesh','11-07-2023','13:20'),
    (1,'manish','11-07-2023','19:20'),
    (2,'rajesh','11-07-2023','17:20'),
    (1,'manish','12-07-2023','10:32'),
    (1,'manish','12-07-2023','12:20'),
    (3,'vikash','12-07-2023','09:12'),
    (1,'manish','12-07-2023','16:23'),
    (3,'vikash','12-07-2023','18:08')
    ]

employee_schema = ['id', 'name', 'date', 'time']
employee = spark.createDataFrame(data=employee_data, schema=employee_schema)\
    .withColumn('timestamp',
                from_unixtime(
                    unix_timestamp(
                        expr("concat(date,' ',time)")
                        ,'dd-MM-yyyy HH:mm')
                    ,'yyyy-MM-dd HH:mm')
                )

employee.display()

#unix_timestamp() reads date and time as a string in a format mentioned by the user; generates a Unix timestamp Converts a string to a Unix timestamp (number of seconds since Jan 1, 1970). 
# from_unixtime() reads the Unix timestamp and converts it into human readable format.
# formats must be specified in exactly same case as here: https://dbmstutorials.com/pyspark/spark-dataframe-format-timestamp.html

# COMMAND ----------

# DBTITLE 1,Q2 solution - Method 1
#calculate office duration for each employee on each day

window_spec = w.partitionBy('id','date').orderBy('timestamp').rowsBetween(w.unboundedPreceding,w.unboundedFollowing)

result = employee\
    .withColumn('timestamp_unix',unix_timestamp(col('timestamp'),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('office_duration'
                ,   (
                    (
                    last('timestamp_unix').over(window_spec)
                    -
                    first('timestamp_unix').over(window_spec)
                    )
                    /
                    (60*60)
                    ).cast(DecimalType(precision=4,scale=2))
                )\
    .select('id','name','date','timestamp','timestamp_unix','office_duration')\
    .sort('id','date')

result.display()

# COMMAND ----------

# DBTITLE 1,Q2 solution - Method 2
#calculate office duration for each employee on each day

window_spec = w.partitionBy('id','date').orderBy('timestamp').rowsBetween(w.unboundedPreceding,w.unboundedFollowing)

result = employee\
    .withColumn('officeIn',col('timestamp').cast(tim),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('officeIn',first('timestamp').over(window_spec))\
    .withColumn('officeOut',last('timestamp').over(window_spec))\
    .withColumn('officeIn',to_timestamp(col('officeIn'),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('officeOut',to_timestamp(col('officeOut'),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('office_duration',(col('officeOut')-col('officeIn')))\
    .distinct()\
    .sort('id','date')

result.display()


    .withColumn('officeIn',date_format(to_timestamp(col('officeIn')),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('officeOut',date_format(to_timestamp(col('officeOut')),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('office_duration',date_format(to_timestamp(col('office_duration')),'yyyy-MM-dd HH:mm:ss'))\
    .select('id','name','date','officeIn','officeOut','office_duration')\
    
