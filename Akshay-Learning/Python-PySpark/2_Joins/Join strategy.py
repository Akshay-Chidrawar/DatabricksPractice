# Databricks notebook source
# DBTITLE 1,Data
dim_customers_data = [
    (1,'manish','patna',"30-05-2022"),
    (2,'vikash','kolkata',"12-03-2023"),
    (3,'nikita','delhi',"25-06-2023"),
    (4,'rahul','ranchi',"24-03-2023"),
    (5,'mahesh','jaipur',"22-03-2023"),
    (6,'prantosh','kolkata',"18-10-2022"),
    (7,'raman','patna',"30-12-2022"),
    (8,'prakash','ranchi',"24-02-2023"),
    (9,'ragini','kolkata',"03-03-2023"),
    (10,'raushan','jaipur',"05-02-2023")
]
dim_customers_schema=['customer_id','customer_name','address','date_of_joining']

dim_products_data = [
    (1, 'fanta',20),
    (2, 'dew',22),
    (5, 'sprite',40),
    (7, 'redbull',100),
    (12,'mazza',45),
    (22,'coke',27),
    (25,'limca',21),
    (27,'pepsi',14),
    (56,'sting',10)
]
dim_products_schema=['product_id','product_name','price']

fact_sales_data = [
    (1,22,10,"01-06-2022"),
    (1,27,5,"03-02-2023"),
    (2,5,3,"01-06-2023"),
    (5,22,1,"22-03-2023"),
    (7,22,4,"03-02-2023"),
    (9,5,6,"03-03-2023"),
    (2,1,12,"15-06-2023"),
    (1,56,2,"25-06-2023"),
    (5,12,5,"15-04-2023"),
    (11,12,76,"12-03-2023")
]
fact_sales_schema=['customer_id','prdct_id','quantity','date_of_sale']

dim_customers = spark.createDataFrame(data=dim_customers_data,schema=dim_customers_schema).sort('customer_id')
dim_products = spark.createDataFrame(data=dim_products_data,schema=dim_products_schema).sort('product_id')
fact_sales = spark.createDataFrame(data=fact_sales_data,schema=fact_sales_schema).sort('customer_id','prdct_id')

s = fact_sales.alias('s')
c = dim_customers.alias('c')

# COMMAND ----------

s.show()
c.show()

# COMMAND ----------

# DBTITLE 1,AQE enabled
spark.conf.set('spark.sql.adaptive.enabled','true')

shuffle_sort_merge = s.join(c,s['customer_id']==c['customer_id'],'inner')
shuffle_sort_merge.collect()

# COMMAND ----------

# DBTITLE 1,AQE disabled
spark.conf.set('spark.sql.adaptive.enabled','false')

shuffle_sort_merge = s.join(c,s['customer_id']==c['customer_id'],'inner')
shuffle_sort_merge.collect()

# COMMAND ----------

# DBTITLE 1,shuffle partitions = 4
spark.conf.set('spark.sql.adaptive.enabled','false')
spark.conf.set('spark.sql.shuffle.partitions',3)

shuffle_sort_merge = s.join(c,s['customer_id']==c['customer_id'],'inner')
shuffle_sort_merge.collect()

# COMMAND ----------

spark.conf.get('spark.sql.autoBroadcastJoinThreshold') #10MB
#you can set this value to -1 in order to restrict broadcast join

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,AQE disabled
spark.conf.set('spark.sql.adaptive.enabled','false')

broadcast_hash_merge = s.join(broadcast(c),s['customer_id']==c['customer_id'],'inner')
broadcast_hash_merge.collect()

# COMMAND ----------

# DBTITLE 1,AQE enabled
spark.conf.set('spark.sql.adaptive.enabled','true')

broadcast_hash_merge = s.join(broadcast(c),s['customer_id']==c['customer_id'],'inner')
broadcast_hash_merge.collect()

# COMMAND ----------

broadcast_hash_merge.explain() # no shuffle 

# COMMAND ----------


