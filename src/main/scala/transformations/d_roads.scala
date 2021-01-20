package transformations

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}


object d_roads {

  val spark: SparkSession = SparkSession.builder()
    .appName("roadsTransformation")
    .getOrCreate()

  def readCsv(path: String) = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path).cache()
  }

  val mainDataNorthEngland : String = s"/user/p_bednarski_pl/project2/mainDataNorthEngland.csv"
  val mainDataScotland :String = s"/user/p_bednarski_pl/project2/mainDataScotland.csv"
  val mainDataSouthEngland :String = s"/user/p_bednarski_pl/project2/mainDataSouthEngland.csv"

  val scotlandRoads_ds = readCsv(mainDataScotland)
  val northEnglandRoads_ds = readCsv(mainDataNorthEngland)
  val southEnglandRoads_ds = readCsv(mainDataSouthEngland)

  case class roadType (id: BigInt,
                       road_category: String,
                       road_type: String)

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val dataUnion = scotlandRoads_ds.select(scotlandRoads_ds("road_category"), scotlandRoads_ds("road_type")).
      union(northEnglandRoads_ds.select(northEnglandRoads_ds("road_category"), northEnglandRoads_ds("road_type")).
        union(southEnglandRoads_ds.select(southEnglandRoads_ds("road_category"), southEnglandRoads_ds("road_type")))).
      distinct()


    val roadsToWrite = dataUnion.withColumn("id", monotonically_increasing_id())
      .select(col("id").alias("id"), col("road_category"), col("road_type")
      ).as[roadType]

  roadsToWrite.write.format("orc").saveAsTable("d_roads")


  }

}
