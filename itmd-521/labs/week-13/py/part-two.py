from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

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
# https://github.com/minio/training/blob/main/spark/taxi-data-writes.py
# https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled','true')
conf.set('spark.hadoop.fs.s3a.committer.name','magic')
# Internal IP for S3 cluster proxy
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

spark = SparkSession.builder.appName("pbiswal1 convert 60.txt to 60.csv").config('spark.driver.host','spark-edge.service.consul').config(conf=conf).getOrCreate()

df = spark.read.csv('s3a://itmd521/60.txt')

splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
    .withColumn('WBAN', df['_c0'].substr(11, 5)) \
    .withColumn('ObservationDate',to_date(df['_c0'].substr(16,8), 'yyyyMMdd')) \
    .withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
    .withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
    .withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
    .withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
    .withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
    .withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
    .withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
    .withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
    .withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
    .withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
    .withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') /10) \
    .withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
    .withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
    .withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
    .withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float')/ 10) \
    .withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

splitDF.printSchema()
splitDF.show(5)

##############################################################################
# Writing the data as a CSV file in the S3 bucket pbiswal1
##############################################################################
splitDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://pbiswal1/60-uncompressed.csv")

# Writing to compressed format of lz4 to S3 bucket
splitDF.write.format("csv").mode("overwrite").option("compression","lz4").option("header","true").save("s3a://pbiswal1/60-compressed.csv")

# Writing to a single partition using coalesce() to S3 bucket
splitDF.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("s3a://pbiswal1/60.csv")

# Writing to a parquet file to S3 bucket
splitDF.write.format("parquet").mode("overwrite").save("s3a://pbiswal1/60.parquet")