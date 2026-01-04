# Databricks notebook source
# MAGIC %md
# MAGIC Implement SCD Type 2 in Data lake (not Delta lake). Thus, upsert or merge commands will not work here in Data lake.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Data
dim_customers_data = [
(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None)
]
dim_customers_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

fact_sales_data = [
(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]
fact_sales_schema = ['sales_id','customer_id','customer_name','sales_date','food_delivery_address','food_delivery_country','food_cost']

dim_customers = spark.createDataFrame(data= dim_customers_data,schema=dim_customers_schema)\
    .sort(col('id'),col('effective_start_date').desc())

fact_sales = spark.createDataFrame(data=fact_sales_data,schema=fact_sales_schema)\
    .sort(col('customer_id'),col('sales_date').desc())

dim_customers.display()
fact_sales.display()

# COMMAND ----------

c = dim_customers.alias('c')
s = fact_sales.alias('s')

renamed_sales = s\
    .select(
        s['customer_id'].alias('id')
        ,s['customer_name'].alias('name')
        ,s['food_delivery_address'].alias('city')
        ,s['food_delivery_country'].alias('country')
        ,s['sales_date'].alias('effective_start_date')
        )\
    .sort(col('id').asc())

rs = renamed_sales.alias('rs')

new_customers = rs\
    .join(c,rs['id']==c['id'],'left_anti')\
    .select(
        'id','name','city','country'
        ,lit('Y').alias('active')
        ,'effective_start_date'
        ,lit(None).alias('effective_end_date')
        )\
    .sort(col('id').asc())

window_spec = Window.partitionBy('id')

latest_sales = rs\
    .withColumn('max_effective_start_date', max(col('effective_start_date')).over(window_spec))\
    .filter(col('max_effective_start_date')==col('effective_start_date'))\
    .drop('max_effective_start_date')

latest_customers = c\
    .filter(col('active')=='Y')\
    .select('id','name','city','country','active','effective_start_date','effective_end_date')

ls = latest_sales.alias('ls')
lc = latest_customers.alias('lc')

updated_customers_newRec = ls\
    .join(lc,(ls['id']==lc['id']) & (lc['id'].isNotNull()) & (ls['city']!=lc['city']),'left_semi')\
    .select(
        'id','name','city','country'
        ,lit('Y').alias('active')
        ,'effective_start_date'
        ,lit(None).alias('effective_end_date')
        )\
    .sort(ls['id'].asc())

new_customers.display()
updated_customers_newRec.display()

updated_customers_newRec_IDList = [row['id'] for row in updated_customers_newRec.select('id').collect()]

uc = updated_customers_newRec.alias('uc')

customers_df_updated = c\
    .join(uc,c['id']==uc['id'],'left')\
    .withColumn('active',
                when (col('id').isin(updated_customers_newRec_IDList),lit('N'))
                .otherwise (col('active'))
                )\
    .withColumn('effective_end_date',
                when (col('id').isin(UpdateCurr_List) m,lit('2025-01-01'))
                .otherwise (col('effective_end_date'))
                )\

updated_customers = c\
    .unionAll(AddNew)\
    
    .sort(col('id').asc(),col('effective_start_date').desc())\
    .display()


# COMMAND ----------

c = dim_customers.alias('c')
s = fact_sales.alias('s')

latest_customers = c\
    .filter(col('active')=='Y')\
    .select('id','name','city','country','active','effective_start_date','effective_end_date')

window_spec = Window.partitionBy('customer_id')

latest_sales = s\
    .withColumn('max_SalesDt', max(col('sales_date')).over(window_spec))\
    .filter(col('max_SalesDt')==col('sales_date'))\
    .select('customer_id','customer_name','food_delivery_address','food_delivery_country','sales_date')

lc = latest_customers.alias('lc')
ls = latest_sales.alias('ls')

status_customers = ls\
    .join(lc,ls['customer_id']==lc['id'],'left')\
    .withColumn(
        'UpdateCurr_AddNew',
        when (lc['id'].isNull(),lit('AddNew'))
        .when (ls['food_delivery_address']!=lc['city'],lit('UpdateCurr + AddNew'))
        .otherwise(lit('NoChange'))
        )\
    .select(
        ls['customer_id'].alias('id')
        ,ls['customer_name'].alias('name')
        ,ls['food_delivery_address'].alias('city')
        ,ls['food_delivery_country'].alias('country')
        ,lit('Y').alias('active')
        ,ls['sales_date'].alias('effective_start_date')
        ,lit(None).alias('effective_end_date')
        ,'UpdateCurr_AddNew'
        )\
    .sort(col('id').asc())

AddNew = status_customers\
    .filter((col('UpdateCurr_AddNew')=='AddNew') | (col('UpdateCurr_AddNew')=='UpdateCurr + AddNew'))\
    .drop(col('UpdateCurr_AddNew'))

UpdateCurr = status_customers\
    .filter((col('UpdateCurr_AddNew')=='UpdateCurr + AddNew'))\
    .select('id')\
    .drop(col('UpdateCurr_AddNew'))\
    .distinct()

type(UpdateCurr)

UpdateCurr_List = [row['id'] for row in UpdateCurr.select('id').collect()]

dim_customers.display()
status_customers.display()
AddNew.display()
UpdateCurr.display()

updated_customers = c\
    .unionAll(AddNew)\
    .withColumn('active',
                when (col('id').isin(UpdateCurr_List),lit('N'))
                .otherwise (col('active'))
                )\
    .withColumn('effective_end_date',
                when (col('id').isin(UpdateCurr_List) m,lit('2025-01-01'))
                .otherwise (col('effective_end_date'))
                )\
    .sort(col('id').asc(),col('effective_start_date').desc())\
    .display()


# COMMAND ----------

\
    .withColumn('city',
                when ((c['active']=='Y') & (c['city']!=l['food_delivery_address']),l['food_delivery_address'])
                .otherwise (c['city'])
                )\
    .withColumn('effective_start_date',
                when ((c['active']=='Y') & (c['city']!=l['food_delivery_address']),l['sales_date'])
                .otherwise (c['effective_start_date'])
                )\
    .withColumn('effective_end_date',
                when ((c['active']=='Y') & (c['city']!=l['food_delivery_address']),l['sales_date'])
                .otherwise (c['effective_end_date'])
                )\
    .select('id','name','city','country','active','effective_start_date','effective_end_date')


# COMMAND ----------

# dim_customers\
    # .withColumn('effective_end_date',
    #             when (col('effective_end_date').isNull(),lit('9999-12-31')))\
    # .display()

# StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("city", StringType(), True),
#     StructField("country", StringType(), True),
#     StructField("active", StringType(), True),
#     StructField("effective_start_date", StringType(), True),
#     StructField("effective_end_date", StringType(), True)
# ])

# StructType([
#     StructField("sales_id", IntegerType(), True),
#     StructField("customer_id", IntegerType(), True),
#     StructField("customer_name", StringType(), True),
#     StructField("sales_date", StringType(), True),
#     StructField("food_delivery_address", StringType(), True),
#     StructField("food_delivery_country", StringType(), True),
#     StructField("food_cost", IntegerType(), True)
# ])

# COMMAND ----------

# DBTITLE 1,Identify whose address changed ; Update Customers table accordingly
c = customers.alias('c')
s = sales.alias('s')

ids_with_updated_address = c.filter()\
    .join(s,c['id']==s['customer_id'],'left')\
    .withColumn('city',
                when (col('c.active')=='Y' & col('c.city')!=col('s.food_delivery_address'),col('s.food_delivery_address'))
                .otherwise (col('c.city'))
                )\
    .withColumn('effective_start_date',
                when (col('c.active')=='Y' & col('c.city')!=col('s.food_delivery_address'),col('s.sales_date'))
                .otherwise (col('c.effective_start_date'))
                )\
    .select('id','name','city','country','active','effective_start_date','effective_end_date')\
    .sort(col('c.id').asc())

ids_with_updated_address.display()

# COMMAND ----------

c = customers.alias('c')
u = ids_with_updated_address.alias('u')

customers_with_oldRecordsUpdated = c\
    .join(u,col('c.id')==col('u.id'),'inner')\
    .filter(col('c.active')=='Y')\
    .withColumn('active',lit('N'))\
    .withColumn('effective_end_date',col('u.effective_start_date'))\
    .display()
