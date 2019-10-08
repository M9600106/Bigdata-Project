# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("dataframes")\
    .getOrCreate()


# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

appledata = spark.read.csv("/FileStore/tables/applestock.csv",inferSchema=True,header=True)

# COMMAND ----------

appledata.printSchema()

# COMMAND ----------

appledata.show()

# COMMAND ----------

appledata.filter("Close < 400").count()

# COMMAND ----------

appledata.summary().show()

# COMMAND ----------

appledata.columns

# COMMAND ----------

appledata.head(2)

# COMMAND ----------


