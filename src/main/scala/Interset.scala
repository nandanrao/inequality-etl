package edu.upf.inequality.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import java.net.URL
import IO._

object Interset {

  val IntersetSchema = StructType(Seq(
    StructField("nl_obs", LongType),
    StructField("pop_area", DoubleType),
    StructField("pop_part", LongType),
    StructField("int_area", DoubleType),
    StructField("nl_area", DoubleType),
    StructField("nl_part", LongType),
    StructField("geometry", StringType),
    StructField("pop_obs", LongType)
  ))

  def readShapeFile(path: String)(implicit sc: SparkContext) =  {
    // get shape files
    val files = listFiles(path, ".+\\.shp$")

    // read each shape file and interpret polygon as point
    sc.parallelize(files)
      .map(new URL(_))
      .map(readPolygonsAsPoints)
      .flatMap(identity)
      .map(_.values.toSeq)
      .map(Row.fromSeq(_))
  }
}
