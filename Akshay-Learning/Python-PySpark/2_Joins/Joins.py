# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Join concepts:
# MAGIC - If there is not .select() clause present, Spark will bring in all columns from both tables in the result (also the join key - twice, one from each table). 
# MAGIC - If there are columns with same name present in parent tables, use table references if you want to explicitly refer such columns; else Spark throws ambiguity error. If you mention select *, then Spark will fetch all columns without any error because internally Spark refers all such columns through unique references. 
# MAGIC - If there are >1 join conditions, treat them same as filter/ where clause. Put each condition in () and use & or | operator as required. 
# MAGIC - If there are NULLs present in join keys, Spark will filter out all such rows from both tables, before performing join. Thus, such records will never come into results. 
# MAGIC
# MAGIC Consider tables tblA (A) and tblB (B) have number of records:
# MAGIC - n(A) = a
# MAGIC - n(B) = b
# MAGIC - n(A∩B) = c ... (c <= min(a,b))
# MAGIC - n(A∪B) = d = a + b - c
# MAGIC
# MAGIC ### Join Types:
# MAGIC https://i.pinimg.com/originals/bc/0c/8b/bc0c8ba4d12051502a68bade9bba4bc5.png
# MAGIC
# MAGIC - INNER : c
# MAGIC
# MAGIC - LEFT OUTER / LEFT : a
# MAGIC - LEFT ANTI (Complement of LEFT join --> a-c)
# MAGIC
# MAGIC - RIGHT OUTER / RIGHT : b
# MAGIC - RIGHT ANTI (Complement of RIGHT join --> b-c)
# MAGIC
# MAGIC - FULL OUTER / FULL : (Union --> d)
# MAGIC - FULL ANTI : (Complement of INNER --> a + b - 2*c)
# MAGIC
# MAGIC - CROSS : (Cross Product --> a * b)
# MAGIC - LEFT SEMI : exactly same as INNER in terms of operation, only difference is that it will display only LEFT table, unlike INNER which displays both tables. However, this could impact number of records in the result. Check below example. 
# MAGIC
# MAGIC - LEFT ANTI  + INNER = LEFT OUTER
# MAGIC - RIGHT ANTI + INNER = RIGHT OUTER
# MAGIC - LEFT ANTI  + INNER + RIGHT ANTI = FULL OUTER
# MAGIC - LEFT ANTI  RIGHT ANTI = FULL ANTI
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Consider tables tblA and tblB have number of records as a and b respectively; and a > b.
# MAGIC
# MAGIC ### Number of records in LEFT join: >= a (record count in LEFT table)
# MAGIC tblA LEFT JOIN tblB
# MAGIC - 1:1 relationship between LEFT and RIGHT tables --> (=a)
# MAGIC - 1:many relationship between LEFT and RIGHT tables --> (>a)
# MAGIC
# MAGIC ### Number of records in INNER join: 
# MAGIC tblA INNER JOIN tblB
# MAGIC - No match found at all --> 0
# MAGIC - 1:1 relationship --> (<= b)
# MAGIC   - Match found for **all** records --> (=b)
# MAGIC   - Match found for **only few** records --> (<b)
# MAGIC - 1:many relationship -->
# MAGIC   - Match found for **all** records --> (>b) ... (may also exceed >a)
# MAGIC   - Match found for **only few** records --> (cannot predict)

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

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

dim_customers.display()
dim_products.display()
fact_sales.display()


# COMMAND ----------

# DBTITLE 1,Inner, Left, Full
s = fact_sales.alias('s')
c = dim_customers.alias('c')

#s = 10, c = 10; inner = 9, left = 10, full = 15
s\
    .join(c,s['customer_id']==c['customer_id'],'full')\
    .sort(s['customer_id'])\
    .display()


# COMMAND ----------

# DBTITLE 1,inner v/s left semi
c\
    .join(s,c['customer_id']==s['customer_id'],'inner')\
    .sort(c['customer_id'],s['prdct_id'])\
    .display()

#(left semi = (select + distinct) MATCHING records from left table)
c\
    .join(s,c['customer_id']==s['customer_id'],'left_semi')\
    .sort(c['customer_id'])\
    .display()

# COMMAND ----------

# DBTITLE 1,Q1
#Details of customers who never made a purchase

#Method1
c\
    .join(s,c['customer_id']==s['customer_id'],'left')\
    .filter(s['customer_id'].isNull())\
    .sort(c['customer_id'])\
    .display()
#Method2 (left anti = (select + distinct) NON MATCHING records from left table)
c\
    .join(s,c['customer_id']==s['customer_id'],'left_anti')\
    .sort(col('customer_id'))\
    .display()


# COMMAND ----------

# DBTITLE 1,Cross Join (expensive)
c\
    .crossJoin(s)\
    .display()
