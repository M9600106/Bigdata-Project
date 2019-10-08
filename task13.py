# Databricks notebook source
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("jason perf")\
    .getOrCreate()

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

input_jason = spark.read.json("/FileStore/tables/names.json")

# COMMAND ----------

namesdf = input_jason.createOrReplaceTempView("names")

# COMMAND ----------

df2 = spark.sql("select * from names where names.lastname = 'Atluri'").show()

# COMMAND ----------


