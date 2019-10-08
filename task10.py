# Databricks notebook source
from pyspark.sql import SparkSession
import pandas as pd





# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("pair rdd avg")\
    .getOrCreate()

# COMMAND ----------

sc =spark.sparkContext

# COMMAND ----------

data = sc.parallelize([("yaswanth","male",4000),("yaswanth","male",3400),("sunny","female",5000),("sunny","female",2000),("neetu","female",3100)])

# COMMAND ----------

data.take(5)

# COMMAND ----------

bykey = data.map(lambda x: (x[ :2],list(x[2: ])))

# COMMAND ----------

bykey.take(4)

# COMMAND ----------

avgsum = bykey.groupByKey()

# COMMAND ----------

result = avgsum.reduce(lambda x:(k,v),a)=>(k,v),a.sum())

# COMMAND ----------


