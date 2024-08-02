package main.scala.assignment_02
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType}
import org.apache.spark.sql.{SparkSession, Dataset, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._

case class IOT_devices_schema (battery_level: Long, c02_level: Long, 
    cca2: String, cca3: String, cn: String, device_id: Long, 
    device_name: String, humidity: Long, ip: String, latitude: Double,
    lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

object IOT_devices_schema {
  implicit def encoder: Encoder[IOT_devices_schema] =
    ExpressionEncoder[IOT_devices_schema]
}

object assignment_02 {
    def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("assignment_02")
        .getOrCreate()
    
    if (args.length < 1) {
        print("Usage: assignment_02 <iot_devices_dataset>")
        sys.exit(1)
    }

    import spark.implicits._

    val iot_dev_file = args(0)
    val iot_devices_ds: Dataset[IOT_devices_schema] = spark.read.json(iot_dev_file).as[IOT_devices_schema]

    // Question 1: Detect failing devices with battery levels below a threshold.
    val threshold_bat_lev = 8
    val failing_devices_ds = iot_devices_ds.filter($"battery_level" < threshold_bat_lev)
    println(s"Failing devices with battery level less than 8:  ${failing_devices_ds.count()}")
    failing_devices_ds.select($"battery_level", $"c02_level", $"device_name").sort($"c02_level").show(5, false)

    // OUTPUT :- Observation - The table displays observations from various devices along with the battery life, CO2 concentration, and individual device names for each. With information about each device's operational data, each row represents a unique device. As an example, the device named "device-mac-111327FkK365" in the first row has a CO2 level of 800 and a battery level of 4. In the same way, the rows that follow list other devices' specifics and condense their unique features.

    // Question 2: Identify offending countries with high levels of CO2 emissions.
    val threshold_co2: Long = 1400
    val high_co2_ds = iot_devices_ds.filter($"c02_level" > threshold_co2)
    val high_co2_country_ds = high_co2_ds.select($"c02_level",$"cn")
                                .groupBy($"cn")
                                .agg(avg($"c02_level")
                                .alias("avg_c02"))
                                .sort($"avg_c02".desc)
    // val high_co2_country_ds = high_co2_df.select($"c02_level",$"cn").groupBy("cn").avg()

    // Show top 5 offending countries with high CO2 emissions
    high_co2_country_ds.show(5, false)

    // OUTPUT:Observation :- Here i have shown 5 rows and The CO2 averages for different countries are shown in the table. Every row represents a separate nation, displaying that nation's name (shortened to "cn") together with the commensurate average CO2 concentration. The first row, for example, shows the average CO2 level of 1593.5 for Saint Vincent and the Grenadines, whereas the second row shows the average CO2 level of 1588.0 for the Solomon Islands. Comparably, the average CO2 levels of the Federated States of Micronesia, Rwanda, and the British Indian Ocean Territory are provided, offering insights on environmental measurements around the globe.
    

    // Question 3: Compute the min and max values for temperature, battery level, CO2, and humidity.
    val iot_minMax_ds = iot_devices_ds.agg(
      min($"temp").alias("min_temperature"),
      max($"temp").alias("max_temperature"),
      min($"battery_level").alias("min_battery_level"),
      max($"battery_level").alias("max_battery_level"),
      min($"c02_level").alias("min_c02_level"),
      max($"c02_level").alias("max_c02_level"),
      min($"humidity").alias("min_humidity"),
      max($"humidity").alias("max_humidity")
    )

    iot_minMax_ds.show(5, false)

    // OUTPUT: Observation :- The lowest and greatest values for a number of environmental parameters are shown in the table. It contains humidity, CO2 levels, battery levels, and minimum and maximum temperatures. For instance, the lowest recorded temperature is 10°C, while the highest is 34°C. There is a range of battery levels (0 to 9) and CO2 levels (800 to 1599 parts per million) available. Furthermore, there is a range in humidity from 25% to 99%. These measures shed light on the variety of environmental circumstances that the dataset records.
    
    // Question 4: Sort and group by average temperature, CO2, humidity, and country
    val iot_avgValues_ds = iot_devices_ds.groupBy($"cca2", $"cn")
      .agg(
        avg($"temp").alias("avg_temperature"),
        avg($"c02_level").alias("avg_c02_level"),
        avg($"humidity").alias("avg_humidity")
      )
      .orderBy(desc("avg_temperature"), desc("avg_c02_level"), desc("avg_humidity"))

    // Show sorted and grouped DataFrame
    iot_avgValues_ds.show(5, false)

    // OUTPUT: Observation :- Here i have shown top 5 rows . The table displays averaged information for several nations, such as average temperature, average CO2 concentration, average humidity, and country codes (cca2) and names (cn). Every row showcases a different country with its own set of measurements. Anguilla (AI), for instance, has an average temperature of roughly 31.14°C, an average CO2 level of roughly 1165.14 ppm, and an average humidity of roughly 50.71%, according to the top row. In a similar manner, the average environmental measurements for Greenland (GL), Gabon (GA), Vanuatu (VU), and Saint Lucia (LC) are listed, providing information on the climate in various areas.
    
    spark.stop()
    }}