# Databricks notebook source
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

spark = SparkSession \
    .builder\
    .appName("work cound >2")\
    .getOrCreate()

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

input_data = sc.textFile("/FileStore/tables/data.txt")

# COMMAND ----------

nonemptylines = input_data.filter(lambda x: len(x)>0)

# COMMAND ----------

words = nonemptylines.flatMap(lambda x: x.split(" "))

# COMMAND ----------

reqwords = words.filter(lambda x: len(x)==2)

# COMMAND ----------

reqwords.collect()
