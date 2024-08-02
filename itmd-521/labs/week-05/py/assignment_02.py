from ctypes import Array
import sys
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql.functions import count,to_timestamp, month,year,weekofyear
from pyspark.sql.functions import col, substring

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment-02 <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession.builder.appName("Assignment02")
             .config("spark.sql.catalogImplementation", "hive").getOrCreate())
    sf_fire_calls_file = sys.argv[1]

    sf_fire_calls_sch = types.StructType([types.StructField('CallNumber', types.IntegerType(), True),
                                    types.StructField('UnitID', types.StringType(), True),
                                    types.StructField('IncidentNumber', types.IntegerType(), True),
                                    types.StructField('CallType', types.StringType(), True), 
                                    types.StructField('CallDate', types.StringType(), True), 
                                    types.StructField('WatchDate', types.StringType(), True),
                                    types.StructField('CallFinalDisposition', types.StringType(), True),
                                    types.StructField('AvailableDtTm', types.StringType(), True),
                                    types.StructField('Address', types.StringType(), True), 
                                    types.StructField('City', types.StringType(), True), 
                                    types.StructField('Zipcode', types.IntegerType(), True), 
                                    types.StructField('Battalion', types.StringType(), True), 
                                    types.StructField('StationArea', types.StringType(), True), 
                                    types.StructField('Box', types.StringType(), True), 
                                    types.StructField('OriginalPriority', types.StringType(), True), 
                                    types.StructField('Priority', types.StringType(), True), 
                                    types.StructField('FinalPriority', types.IntegerType(), True), 
                                    types.StructField('ALSUnit', types.BooleanType(), True), 
                                    types.StructField('CallTypeGroup', types.StringType(), True),
                                    types.StructField('NumAlarms', types.IntegerType(), True),
                                    types.StructField('UnitType', types.StringType(), True),
                                    types.StructField('UnitSequenceInCallDispatch', types.IntegerType(), True),
                                    types.StructField('FirePreventionDistrict', types.StringType(), True),
                                    types.StructField('SupervisorDistrict', types.StringType(), True),
                                    types.StructField('Neighborhood', types.StringType(), True),
                                    types.StructField('Location', types.StringType(), True),
                                    types.StructField('RowID', types.StringType(), True),
                                types.StructField('Delay', types.FloatType(), True)])

    sf_fire_df =  spark.read.csv(sf_fire_calls_file, header=True, schema=sf_fire_calls_sch)
     
    # Qestion 1: What were all the different types of fire calls in 2018?
    (sf_fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .where(col("CallDate").contains('2018'))
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show())
    
    # Answer 1: OUTPUT :- The frequency of various forms of emergency calls is displayed in the table. A call category, such as "Medical Incident," "Alarms," or "Structure Fire," is represented by each row, along with the number of incidents that correspond to that type of call. For instance, the most common category of reports, with 7004 recorded occurrences, was "Medical Incidents," followed by 1144 reports of "Alarms." The information offers important insights into the frequency and distribution of various emergency scenarios that are managed by the various emergency agencies.
    """
    +--------------------+-----+
    |            CallType|count|
    +--------------------+-----+
    |    Medical Incident| 7004|
    |              Alarms| 1144|
    |      Structure Fire|  906|
    |   Traffic Collision|  433|
    |        Outside Fire|  153|
    |               Other|  114|
    |Citizen Assist / ...|  113|
    |Gas Leak (Natural...|   69|
    |        Water Rescue|   43|
    |Elevator / Escala...|   36|
    |   Electrical Hazard|   30|
    |        Vehicle Fire|   28|
    |Smoke Investigati...|   28|
    |Odor (Strange / U...|   10|
    |          Fuel Spill|   10|
    |              HazMat|    5|
    |Train / Rail Inci...|    5|
    |  Suspicious Package|    3|
    |       Assist Police|    1|
    |           Explosion|    1|
    +--------------------+-----+
    """
    # Qestion 2: What months within the year 2018 saw the highest number of fire calls?
    sf_fire_df_ts = (sf_fire_df
                  .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                  .drop("CallDate")
                  .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                  .drop("WatchDate")
                  .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
                                                            "MM/dd/yyyy hh:mm:ss a"))
                  .drop("AvailableDtTm"))
    
    (sf_fire_df_ts
    .select("IncidentDate")
    .where(col("CallType").contains('Fire'))
    .where(year("IncidentDate") == '2018')
    .groupBy(month("IncidentDate"))
    .count()
    .orderBy("count", ascending=False)
    .show(n=1, truncate=False))
    
    #Answer 2:OUTPUT - Observation:- I am showing top 1 row.Among all the occurrences recorded in 2018, October had the highest number of fire calls (120). October's fire-related incident rate appears to have increased during that month, which may be due to certain occurrences or seasonal variables raising the risk of fire. If additional research is done, it might be possible to identify underlying reasons why there are more fire situations, such as trends in the weather, public gatherings, or other environmental variables. Planned preparation, resource allocation, and focused measures to reduce fire risks during peak times can all benefit from an understanding of the temporal distribution of fire events.
      
    
    
    # Qestion 3 : Which neighborhoods in San Francisco generated the most fire calls in 2018?
    (sf_fire_df_ts
    .select("Neighborhood")
    .where(col("CallType").contains('Fire'))
    .where(col("City").contains('SF'))
    .groupBy("Neighborhood")
    .count()
    .orderBy("count", ascending=False)
    .show(n=3, truncate=False))
    
    #OUTPUT: Observation - Here i am showing top 3 records. In 2018, Mission, Tenderloin, and Bayview Hunters Point were the San Francisco neighborhoods that saw the most fire calls. With a total of 2007 reported events, the Mission area had the highest number of fire calls. Tenderloin and Bayview Hunters Point came in second and third, respectively, with 1923 and 1628 incidents. In order to improve safety and reduce the risks associated with fire, these neighborhoods require targeted fire prevention measures, community outreach, and emergency response planning. The data identifies regions that may have higher fire risks or incidence rates.
    
    
    # Qestion 4 : Which neighborhoods had the worst response times to fire calls in 2018?
    (sf_fire_df_ts.select('Neighborhood', 'Delay')
    .filter(col("CallType").contains('Fire'))
    .filter(year("IncidentDate") == '2018')
    .groupBy('Neighborhood')
    .avg('Delay')
    .orderBy(col('avg(Delay)').desc())
    .show(n=10, truncate=False))
    
    #OUTPUT: Observation :- Chinatown experienced the greatest difficulties in responding to fire emergencies in San Francisco in 2018, with an average delay of roughly 21.08 minutes. This extended response time indicates possible bottlenecks or shortcomings in the Chinatown neighborhood's emergency services, requiring immediate attention to improve the efficacy and efficiency of fire response operations. Pacific Heights, Glen Park, and Oceanview/Merced/Ingleside are some of the other areas that are noticeably behind schedule. These results highlight the need of focused interventions and resource distribution to enhance emergency response capacities in a variety of communities, guaranteeing prompt and efficient help during fire occurrences for people' safety and well-being. Here i am showing 10 rows.
    
    
    # Question 5 : Which week in the year in 2018 had the most fire calls?
    (sf_fire_df_ts.select(weekofyear('IncidentDate')
                          .alias('Week'))
     .filter(col("CallType").contains('Fire'))
     .filter(year("IncidentDate") == '2018')
     .groupBy('Week')
     .count()
     .orderBy(col('count').desc())
     .show(n=1, truncate=False))
    
    #OUTPUT:Observation:- The first week of 2018 saw the most number of fire calls. In San Francisco this week, there were 37 recorded fire incidents. This implies that fire-related emergencies occurred more frequently during the start of the year. Insights into seasonal patterns, public behavior, or outside variables impacting fire risk may be gained by analyzing the causes that contributed to this week 1 incident surge. This information might then be used to support focused prevention and preparedness measures to reduce the likelihood of such occurrences in the future.
    
    
    # Question 6:Is there a correlation between neighborhood, zip code, and number of fire calls?
    
    sf_fire_df_ts_corr = sf_fire_df_ts.filter(col("CallType").contains('Fire')).groupBy('Neighborhood', 'Zipcode').count()
    sf_fire_df_ts_corr.show()
          
    #OUTPUT: Observation:- Here i have shown 20 records. The data indicates a clear correlation between San Francisco's zip codes, neighborhoods, and fire call frequency. The number of fire incidents varies by neighborhood, which are denoted by their associated zip codes. For example, Bayview Hunters Point (94124) recorded a significant number of fire calls—69—while Inner Sunset (94122) recorded 308 incidents. However, it is imperative to recognize the existence of null values, which denote absent data and may affect the analysis's thoroughness. Deeper understanding of the strength and kind of the correlation between these factors may be possible with additional investigation using statistical methods.
    
    # Question 7 :How can we use Parquet files or SQL tables to store this data and read it back?
    # Writing DataFrame to Parquet Files
    sf_fire_df_ts.write.mode("overwrite").parquet("./parquet_folder")
    # Reading data from Parquet files
    parquet_sf_fire_df = spark.read.parquet("./parquet_folder")
    parquet_sf_fire_df.show(n=5)
    
    # Register DataFrame as SQL table
    parquet_table = 'sf_fire_table'
    spark.sql(f"DROP TABLE IF EXISTS {parquet_table}")
    sf_fire_df_ts.write.format("parquet").saveAsTable(parquet_table)
    # Reading SQL table
    sql_sf_fire_df = spark.sql("SELECT * FROM sf_fire_table")
    sql_sf_fire_df.show(n=5)
    
    #OUTPUT:Observation:- The dataset that is provided includes information on San Francisco fire events, such as call types, addresses, zip codes, and dates of incidents. After some investigation, other call kinds are discovered, including alarms, medical emergencies, and structural fires. Mission, Tenderloin, and Bayview Hunters Point are the neighborhoods that receive the greatest number of fire calls. Additionally, response times vary noticeably amongst neighborhoods, with Chinatown showing the greatest average delay. Subsequent investigation may reveal associations among zip codes, communities, and fire call frequency, which could facilitate the distribution of resources and emergency response preparations.I have show 5 rows.

    #OUTPUT: Observation : -  Here dataset provides comprehensive details, including call types, addresses, zip codes, and event dates, concerning a variety of San Francisco fire incidents. The details of each row, including the unit ID, neighborhood, and response time, correspond to a particular fire event. The call's final outcome and priority level are among the other details it includes. To help with resource allocation and emergency response planning, additional analysis can identify trends in the kinds and rates of fire occurrences across various neighborhoods and areas.
    
    
    
  