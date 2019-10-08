# Databricks notebook source
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("employees_sort")\
    .getOrCreate()

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

lines = sc.textFile("/FileStore/tables/employees.csv")

# COMMAND ----------

pairrdd = lines.map(lambda x: (x.split(',')[1],x.split(',')[0]))

# COMMAND ----------

pairrdd.take(8)

# COMMAND ----------

sorted = pairrdd.sortByKey()

# COMMAND ----------

sorted.take(5)

# COMMAND ----------



# COMMAND ----------


