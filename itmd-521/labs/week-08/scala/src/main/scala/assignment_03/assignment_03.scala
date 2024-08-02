package main.scala.assignment_03
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType}
import org.apache.spark.sql.{SparkSession, Dataset, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._

object assignment_03 {
    def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("assignment_03")
        .getOrCreate()
    
    if (args.length < 1) {
        print("Usage: assignment_03 <dataset.csv>")
        sys.exit(1)
    }

    val departure_delays_filename = args(0)

    val departure_delays_df = spark.read.format("csv")
    .option("header", "true")
    .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
    .option("path", departure_delays_filename)
    .load()

    // Creating Temp view
    departure_delays_df.createOrReplaceTempView("us_delay_flights_tbl")

    // PART I
    // Spark sql query
    spark.sql("""
        SELECT date, delay, origin, destination
        FROM us_delay_flights_tbl
        WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
        ORDER BY delay DESC
    """).show(10)
    
    // Scala query
    departure_delays_df.select("date", "delay", "origin", "destination")
    .where(col("delay") > 120 && col("origin") === "SFO" && col("destination") === "ORD")
    .orderBy(desc("delay")).show(10)

    // Spark Sql Query
    spark.sql("""
        SELECT delay, origin, destination,
        CASE
        WHEN delay > 360 THEN 'Very Long Delays'
        WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
        WHEN delay = 0 THEN 'No Delays'
        ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC
    """).show(10)

    //Scala query
    departure_delays_df.select(
        col("delay"),
        col("origin"),
        col("destination"),
        when(col("delay") > 360, "Very Long Delays")
        .when(col("delay") > 120 && col("delay") < 360, "Long Delays")
        .when(col("delay") > 60 && col("delay") < 120, "Short Delays")
        .when(col("delay") > 0 && col("delay") < 60, "Tolerable Delays")
        .when(col("delay") === 0, "No Delays")
        .otherwise("Early")
        .alias("Flight_Delays")
    ).orderBy(col("origin"), desc("delay")).show(10)

    // Part II
    // Create database and use that database

    spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
    spark.sql("USE learn_spark_db")

    // Create unmanaged table
    spark.sql(s"""CREATE TABLE IF NOT EXISTS us_delay_flights_tbl(date STRING, delay INT , 
              distance INT, origin STRING, destination STRING) 
              USING csv OPTIONS (PATH '$departure_delays_filename')""")



    // Create temp view with origin ORD and show first 5 records
    spark.sql("DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view")

    spark.sql(
        """CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
            SELECT delay, origin, destination, date,
            month(to_date(date, 'MMddHHmm')) as month_of_date,
            day(to_date(date, 'MMddHHmm')) as day_of_date
            FROM us_delay_flights_tbl
            WHERE origin = 'ORD' AND
            (month(to_date(date, 'MMddHHmm')) = 3) AND
            (day(to_date(date, 'MMddHHmm')) BETWEEN 1 AND 15)
            ORDER BY delay DESC
            """)
         
    spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(5)
    spark.catalog.listColumns("us_delay_flights_tbl").show()
    
    // PART III
  
    val dep_delay_write_df = departure_delays_df
    
    //Write to json file

    dep_delay_write_df.write
    .format("json")
    .mode("overwrite")
    .save("departuredelays.json")

    
    // Write to json file

    dep_delay_write_df.write
    .format("json")
    .option("compression", "lz4")
    .mode("overwrite")
    .save("departuredelays.json.lz4")

    
    //Write to parquet file
    dep_delay_write_df.write
    .format("parquet")
    .mode("overwrite")
    .save("departuredelays.parquet")

  // Part IV
    val par_df = spark.read.format("parquet").load("departuredelays.parquet")
    val orddeparturedelays = par_df.select("date", "delay", "origin", "destination", "distance").where(col("origin") === "ORD")
    orddeparturedelays.show(10)
    orddeparturedelays.write.format("parquet").mode("overwrite").save("orddeparturedelays")


    spark.stop()
}}


