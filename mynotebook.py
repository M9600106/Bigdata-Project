# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("word_count")\
    .getOrCreate()

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

lines = sc.textFile("/FileStore/tables/words.txt")

# COMMAND ----------

words = lines.flatMap(lambda lines: lines.split(" "))

# COMMAND ----------

tuples = words.map(lambda word: (word,1))

# COMMAND ----------

counts = tuples.reduceByKey(lambda a,b: (a+b))

# COMMAND ----------

counts.count()

# COMMAND ----------

counts.take(10)

# COMMAND ----------

counts.saveAsTextFile("/FileStore/tables/wordsoutput.txt")

# COMMAND ----------


