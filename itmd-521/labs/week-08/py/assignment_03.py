from ctypes import Array
import sys
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc, when, unix_timestamp

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment-03 <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession.builder.appName("Assignment03")
             .config("spark.sql.catalogImplementation", "hive").getOrCreate())
    
    departure_delays_filename = sys.argv[1]

    dep_delay_df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", departure_delays_filename)
      .load())
    
    # Create a temporary view
    dep_delay_df.createOrReplaceTempView("us_delay_flights_tbl")
    
    # PART I
    #Spark sql query
    spark.sql("""SELECT date, delay, origin, destination
              FROM us_delay_flights_tbl 
              WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
              ORDER by delay DESC""").show(10)
    
    # Pyspark query
    (dep_delay_df.select("date", "delay", "origin", "destination")
     .where((col("delay") > 120) & (col("origin") == 'SFO') & (col("destination") == 'ORD'))
     .orderBy(col("delay").desc()).show(10))
    
    # Spark Sql Query
    spark.sql("""SELECT delay, origin, destination,
              CASE
              WHEN delay > 360 THEN 'Very Long Delays'
              WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
              WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
              WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
              WHEN delay = 0 THEN 'No Delays'
              ELSE 'Early'
              END AS Flight_Delays
              FROM us_delay_flights_tbl
              ORDER BY origin, delay DESC""").show(10)
    
    # Pyspark Query
    (dep_delay_df.select(
        col("delay"),
        col("origin"),
        col("destination"),
        when(col("delay") > 360, "Very Long Delays")
        .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
        .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
        .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
        .when(col("delay") == 0, "No Delays")
        .otherwise("Early")
        .alias("Flight_Delays"))
     .orderBy("origin", col("delay").desc())
     .show(10))
   
    # PART II
    # Create database and use that database
    spark.sql("CREATE DATABASE  IF NOT EXISTS learn_spark_db")
    spark.sql("USE learn_spark_db")
    
    # Create unmanaged table
    spark.sql("""CREATE TABLE  IF NOT EXISTS us_delay_flights_tbl(date STRING, delay INT, 
              distance INT, origin STRING, destination STRING) 
              USING csv OPTIONS (PATH '""" + departure_delays_filename + """')""")
    
    
    # Create temp view with origin ORD and show first 5 records
    spark.sql("DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view")
    
    spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
              SELECT delay, origin, destination, date, MONTH(TO_DATE(date, 'MMddHHmm')) as month_of_date, DAY(TO_DATE(date, 'MMddHHmm')) as day_of_date
              from us_delay_flights_tbl
              WHERE origin = 'ORD' AND (MONTH(TO_DATE(date, 'MMddHHmm')) = '03') AND (DAY(TO_DATE(date, 'MMddHHmm')) BETWEEN '01' AND '15')
              ORDER BY delay DESC""")
        
    spark.sql("""SELECT * FROM us_origin_airport_JFK_tmp_view""").show(5)
    
    print(spark.catalog.listColumns("us_delay_flights_tbl"))

    # PART III
    dep_delay_write_df = dep_delay_df
    
    # Write to json file
    dep_delay_write_df.write.format('json').mode('overwrite').save('departuredelays.json')
    
    # Write to json file
    dep_delay_write_df.write.format('json').option('compression','lz4').mode('overwrite').save('departuredelays.json.lz4')
    
    # Write to parquet file
    dep_delay_write_df.write.format('parquet').mode('overwrite').save('departuredelays.parquet')
    
    # PART IV
    par_df =  spark.read.format("parquet").load('departuredelays.parquet')
    
    orddeparturedelays = par_df.select("date", "delay", "origin", "destination", "distance").where(col("origin") == 'ORD')
    
    orddeparturedelays.show(10)
    
    orddeparturedelays.write.format("parquet").mode("overwrite").save('orddeparturedelays')
    
    spark.stop()