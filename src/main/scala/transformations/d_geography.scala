package transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._

object d_geography {

  val spark = SparkSession.builder()
    .appName("GeographyTransformation")
    .getOrCreate()


  def readCsv(path: String): DataFrame = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path)
  }

  case class Region(region_id: Int,
                    region_name: String,
                    region_ons_code: String)

  case class Authority(local_authority_ons_code: String,
                       local_authority_id: Int,
                       local_authority_name: String,
                       region_ons_code: String)


  import spark.implicits._

  val scotlandAuthoritiesPath = s"/user/p_bednarski_pl/project2/authoritiesScotland.csv"
  val scotlandRegionsPath = s"/user/p_bednarski_pl/project2/regionsScotland.csv"

  val northEnglandAuthoritiesPath = s"/user/p_bednarski_pl/project2/authoritiesNorthEngland.csv"
  val northEnglandRegionsPath = s"/user/p_bednarski_pl/project2/regionsNorthEngland.csv"

  val southEnglandAuthoritiesPath = s"/user/p_bednarski_pl/project2/authoritiesSouthEngland.csv"
  val southEnglandRegionsPath = s"/user/p_bednarski_pl/project2/regionsSouthEngland.csv"



  def main(args: Array[String]): Unit = {


    val scotlandAuthorities = readCsv(scotlandAuthoritiesPath).as[Authority].cache()
    val scotlandRegions = readCsv(scotlandRegionsPath).as[Region].cache()

    val northEnglandAuthorities = readCsv(northEnglandAuthoritiesPath).as[Authority].cache()
    val northEnglandRegions = readCsv(northEnglandRegionsPath).as[Region].cache()

    val southEnglandAuthorities = readCsv(southEnglandAuthoritiesPath).as[Authority].cache()
    val southEnglandRegions = readCsv(southEnglandRegionsPath).as[Region].cache()


    val dataUnion = scotlandAuthorities.join(scotlandRegions,
      scotlandAuthorities("region_ons_code") === scotlandRegions("region_ons_code")).
      select(
        scotlandAuthorities("local_authority_ons_code"),
        scotlandAuthorities("local_authority_name"),
        scotlandRegions("region_ons_code"),
        scotlandRegions("region_name")
      ).
      union(northEnglandAuthorities.join(northEnglandRegions,
        northEnglandAuthorities("region_ons_code") === northEnglandRegions("region_ons_code")).
        select(
          northEnglandAuthorities("local_authority_ons_code"),
          northEnglandAuthorities("local_authority_name"),
          northEnglandRegions("region_ons_code"),
          northEnglandRegions("region_name")
        ).
        union(southEnglandAuthorities.join(southEnglandRegions,
          southEnglandAuthorities("region_ons_code") === southEnglandRegions("region_ons_code")).
          select(
            southEnglandAuthorities("local_authority_ons_code"),
            southEnglandAuthorities("local_authority_name"),
            southEnglandRegions("region_ons_code"),
            southEnglandRegions("region_name")
          )))

    dataUnion.write.format("orc").saveAsTable("d_geography")

  }
}
