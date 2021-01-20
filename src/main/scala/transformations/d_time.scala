package transformations

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{year, month, dayofmonth, hour, col}

object d_time {

  val spark: SparkSession = SparkSession.builder()
    .appName("timeTransformation")
    .getOrCreate()


  def readCsv(path: String): DataFrame = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", value = true).
      option("inferSchema", value = true).
      csv(path)
  }

  import spark.implicits._

  case class datetime(year: Int,
                      month: Int,
                      day: Int,
                      hour: Int)

  val mainDataNorthEngland : String = s"/FileStore/tables/mainDataNorthEngland_10000.csv"
  val mainDataScotland :String = s"/FileStore/tables/mainDataScotland_10000.csv"
  val mainDataSouthEngland :String = s"/FileStore/tables/mainDataSouthEngland_10000.csv"


  def main(args: Array[String]): Unit = {

    val mainDataNorthEngland_df : DataFrame = readCsv(mainDataNorthEngland).cache()
    val mainDataScotland_df : DataFrame = readCsv(mainDataScotland).cache()
    val mainDataSouthEngland_df : DataFrame = readCsv(mainDataSouthEngland).cache()


    val dataUnion = mainDataNorthEngland_df.select(mainDataNorthEngland_df("count_date"), mainDataNorthEngland_df("hour")).
      union(mainDataScotland_df.select(mainDataScotland_df("count_date"), mainDataScotland_df("hour")).
        union(mainDataSouthEngland_df.select(mainDataSouthEngland_df("count_date"), mainDataSouthEngland_df("hour")))).
      distinct()

    val timestamp = dataUnion.select(
      year(col("count_date")).alias("year"),
      month(col("count_date")).alias("month"),
      dayofmonth(col("count_date")).alias("day"), col("hour")
    ).as[datetime]

    val timetowrite = timestamp.selectExpr("make_timestamp(YEAR, MONTH, DAY, HOUR, 00, 00) as timestamp")
      .select(col("timestamp"), year(col("timestamp")).alias("year"),
      month(col("timestamp")).alias("month"),
      dayofmonth(col("timestamp")).alias("day"), hour(col("timestamp")).alias("hour")) : DataFrame

    timetowrite.write.format("orc").saveAsTable("d_time")





  }

}
