package transformations

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window


object f_main {

  val spark: SparkSession = SparkSession.builder()
    .appName("timeTransformation")
    .enableHiveSupport()
    .getOrCreate()


  def readCsv(path: String): DataFrame = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", value = true).
      option("inferSchema", value = true).
      csv(path)
  }

  import spark.implicits._

  case class WeatherFromFile(region: String,
                             date: String,
                             time: String,
                             conditions: String)

  case class factsMain(count_date: Timestamp,
                       hour: Int,
                       local_authoirty_ons_code: String,
                       road_category: String,
                       pedal_cycles: Int,
                       two_wheeled_motor_vehicles: Int,
                       cars_and_taxis: Int,
                       buses_and_coaches: Int,
                       lgvs: Int,
                       hgvs_2_rigid_axle: Int,
                       hgvs_3_rigid_axle: Int,
                       hgvs_4_or_more_rigid_axle: Int,
                       hgvs_3_or_4_articulated_axle: Int,
                       hgvs_5_articulated_axle: Int,
                       hgvs_6_articulated_axle: Int
                      )


  val weatherFile = spark.sparkContext.textFile(s"/FileStore/tables/weather_100000.txt")
  val linesRdd = weatherFile.flatMap(_.split("\n"))

  val mainDataNorthEngland : String = s"/FileStore/tables/mainDataNorthEngland_10000.csv"
  val mainDataScotland :String = s"/FileStore/tables/mainDataScotland_10000.csv"
  val mainDataSouthEngland :String = s"/FileStore/tables/mainDataSouthEngland_10000.csv"


  def main(args: Array[String]): Unit = {

    val mainDataNorthEngland_df : DataFrame = readCsv(mainDataNorthEngland).cache()
    val mainDataScotland_df : DataFrame = readCsv(mainDataScotland).cache()
    val mainDataSouthEngland_df : DataFrame = readCsv(mainDataSouthEngland).cache()

    val capturePattern =
      """In the region of ([A-Z0-9]+|null) on ([0-9]{2}\/[0-9]{2}\/[0-9]{4}|null) at ([0-9:]+|null) the following weather conditions were reported: ([A-Za-z ]+|null)""".r


    val matches = linesRdd.map(line => {
      val capturePattern(region, date, time, conditions) = line
      WeatherFromFile(region, date, time, conditions)
    })

    val matchesDS = matches.toDS
      .withColumn("timestamp", to_timestamp(concat($"date",lit(" "),$"time"),"MM/dd/yyyy HH:mm"))


    val dataUnion = mainDataNorthEngland_df
      .unionAll(mainDataSouthEngland_df)
      .unionAll(mainDataScotland_df)
      .drop($"count_point_id")
      .drop($"direction_of_travel")
      .drop($"year")
      .drop($"road_name")
      .drop($"road_type")
      .drop($"start_junction_road_name")
      .drop($"end_junction_road_name")
      .drop($"easting")
      .drop($"northing")
      .drop($"latitude")
      .drop($"longitude")
      .drop($"link_length_km")
      .drop($"link_length_miles")
      .drop($"all_hgvs")
      .drop($"all_motor_vehicles")
      .as[factsMain]
      .withColumn("timestamp", (unix_timestamp(date_format(col("count_date"),"yyyy-MM-dd") , "yyyy-MM-dd")
        .as("timestamp")+ $"hour" * 60 * 60).cast(TimestampType))
      .drop($"hour")
      .drop($"count_date")


    val d_roads = spark.table("d_roads")
    val d_vehicles = spark.table("d_vehicles")


    val vehicles_type_list = d_vehicles.select("vehicle_type").map(r => r.getString(0)).collect.toList

    val vehiclesLisstData: Column = coalesce(
      vehicles_type_list.map(c => when(d_vehicles("vehicle_type") === c, col(c)).otherwise(lit(null))): _*)


    val weatherData = matchesDS.select($"region", $"timestamp", $"conditions")
    val window = Window.partitionBy("region").orderBy("timestamp")

    val prevDate = lag(col("timestamp"), 1).over(window)
    val nextDate = lead(col("timestamp"), 1).over(window)

    val data2 = weatherData.withColumn("prevDate", prevDate).withColumn("nextDate", nextDate)

    val data3 = dataUnion.join(data2, data2("region") === dataUnion("local_authoirty_ons_code")
      and data2("prevDate") <= dataUnion("timestamp")
      and data2("nextDate") > dataUnion("timestamp"))
      .select(dataUnion("local_authoirty_ons_code"),
        data2("region"),
        data2("conditions"),
        dataUnion("timestamp").alias("data_timestamp"),
        data2("timestamp").alias("weather_timestamp"))






    val facts =  dataUnion.join(d_roads, d_roads("road_category") === dataUnion("road_category"))
      .select(dataUnion("*"), d_roads("id").alias("road_category"))
      .crossJoin(d_vehicles).select(col("id").alias("vehicle_id"), (vehiclesLisstData).alias("vehicle_count"), $"*")
      .drop(dataUnion("road_category"))
      .drop(dataUnion("pedal_cycles"))
      .drop(dataUnion("two_wheeled_motor_vehicles"))
      .drop(dataUnion("cars_and_taxis"))
      .drop(dataUnion("buses_and_coaches"))
      .drop(dataUnion("lgvs"))
      .drop(dataUnion("hgvs_2_rigid_axle"))
      .drop(dataUnion("hgvs_3_rigid_axle"))
      .drop(dataUnion("hgvs_4_or_more_rigid_axle"))
      .drop(dataUnion("hgvs_3_or_4_articulated_axle"))
      .drop(dataUnion("hgvs_5_articulated_axle"))
      .drop(dataUnion("hgvs_6_articulated_axle"))
      .drop(d_vehicles("vehicle_type"))
      .drop(d_vehicles("vehicle_category"))
      .drop(d_vehicles("has_engine"))
      .drop(d_vehicles("id"))



    facts.write.format("orc").saveAsTable("facts")


  }

}
