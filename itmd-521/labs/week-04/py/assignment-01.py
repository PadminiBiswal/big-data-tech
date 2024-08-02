from ctypes import Array
import sys
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import count
from pyspark.sql.functions import col

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment-01 <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession.builder.appName("Assignment01").getOrCreate())
    divvy_trips_file = sys.argv[1]

    dt_df_inf_sch = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(divvy_trips_file))
    
    print('Data read with inferred Schema')
    dt_df_inf_sch.printSchema()
    print('Row count from inferred schema DF', dt_df_inf_sch.count())

    cust_schema = types.StructType([
      types.StructField("trip_id", types.IntegerType(), nullable = True),
      types.StructField("starttime", types.StringType(), nullable = True),
      types.StructField("stoptime", types.StringType(), nullable = True),
      types.StructField("bikeid", types.IntegerType(), nullable = True),
      types.StructField("tripduration", types.IntegerType(), nullable = True),
      types.StructField("from_station_id", types.IntegerType(), nullable = True),
      types.StructField("from_station_name", types.StringType(), nullable = True),
      types.StructField("to_station_id", types.IntegerType(), nullable = True),
      types.StructField("to_station_name", types.StringType(), nullable = True),
      types.StructField("usertype", types.StringType(), nullable = True),
      types.StructField("gender", types.StringType(), nullable = True),
      types.StructField("birthyear", types.IntegerType(), nullable = True)
    ])

    dt_df_cust_sch = (spark.read
                .option("header", "true")
                .schema(cust_schema)
                .csv(divvy_trips_file))
    
    print('Data read with custom Schema using StructType')
    dt_df_cust_sch.printSchema()
    print('Row count from custom schema DF',dt_df_cust_sch.count())
    
    ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    dt_df_ddl_sch = (spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .schema(ddl_schema)
                .csv(divvy_trips_file))
    
    print('Data read with DDL Schema')
    dt_df_ddl_sch.printSchema()
    print('Row count from DDL schema DF',dt_df_ddl_sch.count())

    female_only_df = dt_df_ddl_sch.filter(col("gender") == "Female")

    female_only_df.groupBy("to_station_name").count().show(10)

    spark.stop()
    