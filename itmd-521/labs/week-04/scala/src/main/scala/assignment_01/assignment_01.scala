package main.scala.assignment_01
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object assignment_01 {
def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("assignment_01")
        .getOrCreate()
    
    if (args.length < 1) {
        print("Usage: assignment_01 <Divvy_Trips_dataset>")
        sys.exit(1)
    }
    
    val divvy_trips_file = args(0)
    val dt_df_inf_sch = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(divvy_trips_file)

    println(s"Data read with inferred Schema")
    dt_df_inf_sch.printSchema()
    println(s"Row count from inferred schema DF ${dt_df_inf_sch.count()}")
    println()

    val cust_schema = StructType(Array(
      StructField("trip_id", IntegerType, false),
      StructField("starttime", StringType, false),
      StructField("stoptime", StringType, false),
      StructField("bikeid", IntegerType, false),
      StructField("tripduration", IntegerType, false),
      StructField("from_station_id", IntegerType, false),
      StructField("from_station_name", StringType, false),
      StructField("to_station_id", IntegerType, false),
      StructField("to_station_name", StringType, false),
      StructField("usertype", StringType, false),
      StructField("gender", StringType, false),
      StructField("birthyear", IntegerType, false)
    ))

    val dt_df_cust_sch = (spark.read
                .option("header", "true")
                .schema(cust_schema)
                .csv(divvy_trips_file))
    
    println(s"Data read with custom Schema using StructType")
    dt_df_cust_sch.printSchema()
    println(s"Row count from custom schema DF ${dt_df_cust_sch.count()}")
    
    val ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    val dt_df_ddl_sch = (spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .schema(ddl_schema)
                .csv(divvy_trips_file))
    
    println(s"Data read with DDL Schema")
    dt_df_ddl_sch.printSchema()
    println(s"Row count from DDL schema DF ${dt_df_ddl_sch.count()}")

    val female_only_df = dt_df_ddl_sch.filter(col("gender") === "Female")

    female_only_df.groupBy("to_station_name").count().show(10)

    spark.stop()
}
}