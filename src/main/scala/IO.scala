package edu.upf.inequality.etl

import geotrellis.shapefile._
import ShapeFileReader.SimpleFeatureWrapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.shapefile._
import java.net.{URL, URI}
import geotrellis.vector._
import org.geotools.data.simple._
import org.opengis.feature.simple._
import org.geotools.data.shapefile._
import com.vividsolutions.jts.{geom => jts}
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import Interset._
import magellan.{Polygon => MgPolygon}
import geotrellis.vector._

object IO {

  case class Geometry(`type`: String, coordinates: Seq[Double])

  def readCsvs(path: String)(implicit sqlContext: SQLContext, sc: SparkContext) = {
    val files = listFiles(path, ".+\\.csv$")
    files.map(readOneCsv).reduce(_.union(_))
  }

  def readOneCsv(path: String)(implicit sqlContext: SQLContext) : DataFrame = {
    val re = """(\d+)\.csv""".r
    val part = re.findAllIn(path).matchData.toList(0).group(1)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    df.withColumn("part", lit(part.toLong))
  }

  def listFiles(path: String, pattern: String)(implicit sc: SparkContext) = {
    FileSystem
      .get(new URI(path), sc.hadoopConfiguration)
      .listStatus(new Path(path))
      .map(_.getPath().toString())
      .filter(x => x matches pattern)
  }

  // magellan conversion to jts.polygon
  implicit def magellanConversion(poly: MgPolygon) : Polygon = {
    Polygon(poly.xcoordinates
      .zip(poly.ycoordinates)
      .map(Point(_)).toSeq)
  }

  def readAsGeometry(r: Row) : Option[Geometry] = {
    r.getAs[MgPolygon](2).centroid.as[Point].map(p => Geometry("Point", Seq(p.x, p.y)))
  }

  def readShapeFile(path: String)(implicit spark: SparkSession) : RDD[Interset] = {
    spark.read.format("magellan")
      .load(path).select("polygon")
      .rdd
      .map(r => {
        Map[String,Object]("geometry" -> readAsGeometry(r)) ++ r.getAs[Map[String, Object]](3)
      })
      .map(castToInterset)
  }
}
