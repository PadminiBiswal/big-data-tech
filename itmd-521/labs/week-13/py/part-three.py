from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import year, month, col

# Removing hard coded password - using os module to import them
import os
import sys

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('SECRETKEY'))
# Configure these settings
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled','true')
conf.set('spark.hadoop.fs.s3a.committer.name','magic')
# Internal IP for S3 cluster proxy
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

spark = SparkSession.builder.appName("pbiswal1 part-three avg-tmp").config('spark.driver.host','spark-edge.service.consul').config(conf=conf).getOrCreate()

df = spark.read.parquet('s3a://pbiswal1/60.parquet')

df.printSchema()
df.show(5)

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.month.html
newDF = df.withColumn("year",year(col("ObservationDate"))).withColumn("month", month(col("ObservationDate")))

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html
avgTempDF = newDF.groupBy("year","month").agg({"AirTemperature": "avg"}).alias("AvgAirTemp")

avgTempDF.write.format("parquet").mode("overwrite").save("s3a://pbiswal1/part-three.parquet")

# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.head.html#pandas.DataFrame.head
avgTempDF.coalesce(1).limit(12).write.format("csv").mode("overwrite").option("header","true").save("s3a://pbiswal1/part-three.csv")