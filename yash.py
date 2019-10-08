# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql import Row
import pandas as pd
import datetime
from pyspark.sql.functions import year, month, dayofmonth


# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("patients")\
    .getOrCreate()


# COMMAND ----------

sc = spark.sparkContext



# COMMAND ----------

data = sc.textFile("/FileStore/tables/patients.txt")


# COMMAND ----------

datadf = data.map(lambda a: Row(a.split(",")[0] ,a.split(",")[1] ,a.split(",")[2] ,a.split(",")[3]))

# COMMAND ----------

reqdf = datadf.toDF(['pid','pname','pdob','plastvisit'])

# COMMAND ----------

reqdf.take(4)

# COMMAND ----------

filtereddf = reqdf.select('pid','pname','pdob',year('pdob').alias('yearofbirth'),month('pdob').alias('monthofbirth'),dayofmonth('pdob').alias('day'),'plastvisit')

# COMMAND ----------

tempdf = filtereddf.createOrReplaceTempView("patient")

# COMMAND ----------

df2 = spark.sql("select * from patient")

# COMMAND ----------

df2.show()

# COMMAND ----------

df3 =spark.sql("select * from patient where TO_DATE(CAST(UNIX_TIMESTAMP(plastvisit, 'yyyy-mm-dd')as timestamp))BETWEEN '2012-09-15' and CURRENT_timestamp() order by plastvisit")

# COMMAND ----------

df3.show()

# COMMAND ----------

df4=spark.sql("select * from patient where yearofbirth=2011")

# COMMAND ----------

df4.show()

# COMMAND ----------



# COMMAND ----------


