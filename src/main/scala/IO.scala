package edu.upf.inequality.etl

import geotrellis.shapefile._
import ShapeFileReader.SimpleFeatureWrapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.shapefile._
import java.net.URL
import geotrellis.vector._
import org.geotools.data.simple._
import org.opengis.feature.simple._
import org.geotools.data.shapefile._
import com.vividsolutions.jts.{geom => jts}
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.SparkContext


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
      .get(sc.hadoopConfiguration)
      .listStatus(new Path(path))
      .map(_.getPath().toString())
      .filter(x => x matches pattern)
  }

  def readSimpleFeatures(url: URL) = {
    // Extract the features as GeoTools 'SimpleFeatures'
    val ds = new ShapefileDataStore(url)
    val ftItr: SimpleFeatureIterator = ds.getFeatureSource.getFeatures.features

    try {
      val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
      while(ftItr.hasNext) simpleFeatures += ftItr.next()
      simpleFeatures.toList
    } finally {
      ftItr.close
      ds.dispose
    }
  }

  def readPolygonsAsPoints(url: URL) : List[Map[String, Object]]= {
    readSimpleFeatures(url)
      .flatMap{ ft => ft.geom[jts.MultiPolygon]
        .map(_.getCentroid)
        .map(x => Geometry("Point", Seq(x.getX, x.getY)))
        .map(x => Map("geometry" -> x) ++ ft.attributeMap) }
  }
}
