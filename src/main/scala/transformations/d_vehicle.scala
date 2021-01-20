package transformations

import org.apache.spark.sql._



object d_vehicle {

  val spark: SparkSession = SparkSession.builder()
    .appName("vehiclesTransformation")
    .getOrCreate()

  import spark.implicits._


  case class Vehicle (
                       id: BigInt,
                       vehicle_type: String,
                       vehicle_category: String,
                       has_engine: Boolean
                     )

  def main(args: Array[String]): Unit = {

    val vehicles =
      Seq(Vehicle(1,"pedal_cycles","pedal_cycles",false),
        Vehicle(2,"two_wheeled_motor_vehicles","two_wheeled_motor_vehicles",true),
        Vehicle(3,"cars_and_taxis","cars_and_taxis",true),
        Vehicle(4,"buses_and_coaches","buses_and_coaches",true),
        Vehicle(5,"lgvs","lgvs",true),
        Vehicle(6,"hgvs_2_rigid_axle","hgvs",true),
        Vehicle(7,"hgvs_3_rigid_axle","hgvs",true),
        Vehicle(8,"hgvs_4_or_more_rigid_axle","hgvs",true),
        Vehicle(9,"hgvs_3_or_4_articulated_axle","hgvs",true),
        Vehicle(10,"hgvs_5_articulated_axle","hgvs",true),
        Vehicle(11,"hgvs_6_articulated_axle","hgvs",true)
      ).toDS()

    vehicles.write.format("orc").saveAsTable("d_vehicles")




  }

}
