package edu.upf.inequality.etl

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
    geometry: Option[Geometry],
    pop_obs: Option[Int]
  )


  // // All our longs should be null if they don't exist because they're id's of sorts
  implicit def toInt(a:String) : Option[Int] = { Try(a.trim.toInt).toOption }

  def castToInterset(m: Map[String, String], g: Option[Geometry]) : Interset = {
    Interset(
      m("nl_obs"),
      m("pop_area").toDouble,
      m("pop_part"),
      m("int_area").toDouble,
      m("nl_area").toDouble,
      m("nl_part"),
      g,
      m("pop_obs")
    )
  }
}
