# Databricks notebook source
# MAGIC %md
# MAGIC ### Window functions
# MAGIC - window function --> partitionBy is mandatory, orderBy is not. 
# MAGIC
# MAGIC ### orderBy clause: 
# MAGIC - required whenever order of elements of the window are considered for calcuation. Eg - **rank** functions, **cumulative** scenarios, **lead & lag** functions. orderBy is not required for regular sum(), count(), etc functions. 
# MAGIC - When we use orderBy, by default the window slides between (unboundedPreceding, current row).
# MAGIC
# MAGIC - cumulative scenarios calculate values based on ranking of the records. Records need to be sorted on some order (eg- salary). In case 2 or more records have same ranking, cumulative sum() will calculate for all those records together and assign that value common to all those records. Refer below example. 
# MAGIC - rank functions --> rank() assigns 1,2,2,4 ; dense_rank() assigns 1,2,2,3
# MAGIC - row_number() --> gives pure row number (order of items or duplicates not considered)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window as w

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
# Question: For overall organization level:
# 1. sort employees based on their salary.
# 2. find cumulative salary.

window_spec = w.orderBy(col('salary').desc())

result = employee\
    .select('id','name','department','salary')\
    .withColumn('row_number',row_number().over(window_spec))\
    .withColumn('rank',rank().over(window_spec))\
    .withColumn('dense_rank',dense_rank().over(window_spec))\
    .withColumn('cumulative_salary',sum('salary').over(window_spec))\

result.display()

# Question - for each department:
# 1. calculate budget.
# 2. sort employees based on their salary.
# 3. find cumulative salary.

window_spec1 = w.partitionBy('department')
#spark is not sorting salaries; for each department, sum() function adds up all salaries at once.

window_spec2 = w.partitionBy('department').orderBy(col('salary').desc())
#for each department, orderBy clause forces spark to sort salaries, before sum() function calculates running sum;

#if we use .groupBy and .agg, it shrinks the dataframe.
#use window functions to avoid such shrinking.
result1 = employee\
    .select('id','name','department','salary')\
    .withColumn('row_number',row_number().over(window_spec2))\
    .withColumn('dept_rank',rank().over(window_spec2))\
    .withColumn('dept_dense_rank',dense_rank().over(window_spec2))\
    .withColumn('dept_budget',sum('salary').over(window_spec1))\
    .withColumn('dept_cumulative_salary',sum('salary').over(window_spec2))

result1.display()



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
# MAGIC - (col_name, jump, value_for_N_record)
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
    .withColumn('Month-1_sales',lag(col('units'),1,'0').over(window_spec2))\
    .withColumn('Month+2_sales',lead(col('units'),2,'0').over(window_spec2))\
    .withColumn('total_sales',sum(col('units')).over(window_spec1))\
    .withColumn('monthly_contri_ratio',
                when (col('total_sales')==0, lit('NA'))
                .otherwise(
                    concat(
                    ((col('units')/col('total_sales'))*100).cast(DecimalType(precision=5,scale=2))
                    ,lit(' %')
                    )
                )
                )\
    .withColumn('Month-1_change_ratio',
                when (col('Month-1_sales')==0, lit('NA'))
                .otherwise(
                    concat(
                    (((col('units')-col('Month-1_sales'))/col('Month-1_sales'))*100).cast(DecimalType(precision=5,scale=2))
                    ,lit(' %')
                    )
                )
                )\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row between
# MAGIC - when we use orderBy() in window definition, **by default the window slides between (unboundedPreceding, current row)**. We can change this limit using rowsBetween() function:
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

result.distinct().display()

# COMMAND ----------

# DBTITLE 1,Q1a solution
# for a given dimension "dim_name", calculate rolling sales (forward or backward) for given "n" months.
dim_name = 'product_name'

# Backward (similar to lag function, but doing a sum)
n1=3
n_back = -(n1-1)

#Forward (similar to lead function, but doing a sum)
n2=3
n_fwd = (n2-1)

col_name_back = 'RollBack '+str(n1)+' months_Sales'
col_name_fwd = 'RollFwd ' +str(n2)+' months_Sales'
col_name_backfwd = 'RollBackFwd ' +str(n2)+' months_Sales'

# when we use orderBy(), by default rowsBetween() slides over a window of (unboundedPreceding, current row).
# However, in below example, we need to slide over a limited window to access (last 2 months + latest month) & (latest month + next 2 months).

window_spec_p = w.partitionBy(dim_name)
window_spec_po = w.partitionBy(dim_name).orderBy('sales_date')
window_spec_back = w.partitionBy(dim_name).orderBy('sales_date').rowsBetween(n_back,0)
window_spec_fwd = w.partitionBy(dim_name).orderBy('sales_date').rowsBetween(0,n_fwd)
window_spec_backfwd = w.partitionBy(dim_name).orderBy('sales_date').rowsBetween(n_back,n_fwd)

result = sales\
    .withColumn('Month-3_Sales',lag(col('sales'),3,0).over(window_spec_po))\
    .withColumn('Month+3_Sales',lead(col('sales'),3,0).over(window_spec_po))\
    .withColumn('cumulative_sales',sum(col('sales')).over(window_spec_po))\
    .withColumn('product_total_sales',sum(col('sales')).over(window_spec_p))\
    .withColumn(col_name_back,sum(col('sales')).over(window_spec_back))\
    .withColumn(col_name_fwd,sum(col('sales')).over(window_spec_fwd))\
    .withColumn(col_name_backfwd,sum(col('sales')).over(window_spec_backfwd))\
    .select(dim_name,'sales_date','sales','Month-3_Sales','Month+3_Sales','cumulative_sales','product_total_sales',col_name_back,col_name_fwd,col_name_backfwd)

result.display()

# use rangeBetween() to calculate rolling sales for a given range of timeline.
# Here, we use actual row values for comparing ranges, unlike row_number ofas done in rowsBetween().

sales2 = sales\
    .withColumn("sales_date", to_date("sales_date", "dd-MM-yyyy"))\
    .withColumn("sales_yyyymm", date_format("sales_date", "yyyyMM").cast("int"))

window_spec_back = w.partitionBy(dim_name).orderBy('sales_yyyymm').rangeBetween(n_back,0)
window_spec_fwd = w.partitionBy(dim_name).orderBy('sales_yyyymm').rangeBetween(0,n_fwd)
window_spec_backfwd = w.partitionBy(dim_name).orderBy('sales_yyyymm').rangeBetween(n_back,n_fwd)

result2 = sales2\
    .withColumn(col_name_back,sum(col('sales')).over(window_spec_back))\
    .withColumn(col_name_fwd,sum(col('sales')).over(window_spec_fwd))\
    .withColumn(col_name_backfwd,sum(col('sales')).over(window_spec_backfwd))\
    .select(dim_name,'sales_yyyymm','sales',col_name_back,col_name_fwd,col_name_backfwd)

result2.display()

# import builtins
# py_abs = builtins.abs #abs method is available in both Python and PySpark. However, we need to use Python's abs() method, since PySpark's abs() method works on columns, but not on variables. 
# str(py_abs(n) --> gives absolute value of n

    # .withColumn('row_number',row_number().over(window_spec))\
    # .withColumn(col_name_back+'_filtered',
    #             when ((col('row_number')<=n1-1),None)
    #             .otherwise(col(col_name_back))
    #             )\
    # .withColumn(col_name_fwd+'_filtered',
    #             when ((col('row_number')>n2-1),None)
    #             .otherwise(col(col_name_fwd))
    #             )\
    # ,'row_number',col_name_back+'_filtered',col_name_fwd+'_filtered'

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
employee = spark.createDataFrame(data=employee_data, schema=employee_schema)
employee.display()

#unix_timestamp() reads date and time as a string in a format mentioned by the user; generates a Unix timestamp Converts a string to a Unix timestamp (number of seconds since Jan 1, 1970). 
# from_unixtime() reads the Unix timestamp and converts it into human readable format.
# formats must be specified in exactly same case as here: https://dbmstutorials.com/pyspark/spark-dataframe-format-timestamp.html

# COMMAND ----------

# DBTITLE 1,Q2 solution - Method 1
#calculate office duration for each employee on each day

window_spec = w.partitionBy('id','date').orderBy('timestamp').rowsBetween(w.unboundedPreceding,w.unboundedFollowing)

result = employee\
    .withColumn('timestamp',
                from_unixtime(
                    unix_timestamp(
                        expr("concat(date,' ',time)")
                        ,'dd-MM-yyyy HH:mm')
                    ,'yyyy-MM-dd HH:mm')
                )\
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

# MAGIC %md
# MAGIC
# MAGIC ### User Defined Function (UDF):
# MAGIC - If we need to apply a custom function in PySpark, which is purely written in Python, we need to apply UDF method on it. 
# MAGIC - Steps:
# MAGIC 1. This registers the custom Python function with PySpark 
# MAGIC 2. Spark collects required data from JVM (serializes) and converts it into Python readable format. Then, it sends data to Python interpreter using Py4J.
# MAGIC 3. Python interpreter performs necessary operations and sends data back to Spark.

# COMMAND ----------

# DBTITLE 1,Q2 solution - Method 2
#calculate office duration for each employee on each day

def String_To_Timestamp(col_date,col_time):
    return to_timestamp(
        concat_ws(' ',col_date,col_time)
        ,'dd-MM-yyyy HH:mm'
        )

def Timestamp_To_DateTime(col_timestamp):
    return date_format(
        to_timestamp(
            col_timestamp
            ,'dd-MM-yyyy HH:mm'
            )
        ,'yyyy-MM-dd HH:mm:ss'
        )

def Timestamp_To_UnixSec(col_timestamp):
    return unix_timestamp(col_timestamp)

def UnixSec_To_HHmm(col_UnixSec):
    hours = (col_UnixSec % 86400) // 3600
    minutes = (col_UnixSec % 3600) // 60
    secs = col_UnixSec % 60
    return f"{hours:02}:{minutes:02}:{secs:02}"

UnixSec_To_HHmm_UDF = udf(UnixSec_To_HHmm, StringType())
print(UnixSec_To_HHmm_UDF)

result = employee\
    .withColumn('timestamp',String_To_Timestamp(col('date'),col('time')))

window_spec = w.partitionBy('id','date').orderBy('timestamp').rowsBetween(w.unboundedPreceding,w.unboundedFollowing)

result2 = result\
    .withColumn('officeIn',first('timestamp').over(window_spec))\
    .withColumn('officeOut',last('timestamp').over(window_spec))\
    .withColumn('office_duration',(Timestamp_To_UnixSec('officeOut') - Timestamp_To_UnixSec('officeIn')))

result3 = result2\
    .withColumn('officeIn',Timestamp_To_DateTime(col('officeIn')))\
    .withColumn('officeOut',Timestamp_To_DateTime(col('officeOut')))\
    .withColumn('office_duration',UnixSec_To_HHmm_UDF(result2['office_duration']))\
    .select('id','date','officeIn','officeOut','office_duration')\
    .distinct()\
    .sort('id','date')
result3.display()
