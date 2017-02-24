package edu.upf.inequality.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import java.net.URL
import scala.util.Try
import IO._

object Interset {

  case class Interset(
    nl_obs: Option[Int],
    pop_area: Double,
    pop_part: Option[Int],
    int_area: Double,
    nl_area: Double,
    nl_part: Option[Int],
    geometry: Geometry,
    pop_obs: Option[Int]
  )

  // everything that's a double should be 0 if it doesn't exist?
  implicit def toDouble(a:Object) : Double = { Try(a.asInstanceOf[Double]).getOrElse(0.0) }

  // All our longs should be null if they don't exist because they're id's of sorts
  implicit def toInt(a:Object) : Option[Int] = { Try(a.asInstanceOf[Int]).toOption }

  def castToInterset(m: Map[String, Object]) : Interset = {
    Interset(
      m("nl_obs"),
      m("pop_area"),
      m("pop_part"),
      m("int_area"),
      m("nl_area"),
      m("nl_part"),
      m("geometry").asInstanceOf[Geometry],
      m("pop_obs")
    )
  }

  def readShapeFile(path: String)(implicit sc: SparkContext) : RDD[Interset] = {
    // get shape files
    val files = listFiles(path, ".+\\.shp$")

    // read each shape file and interpret polygon as point
    sc.parallelize(files)
      .map(new URL(_))
      .map(readPolygonsAsPoints)
      .flatMap(identity)
      .map(castToInterset)
  }
}
