package edu.upf.inequality.etl

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import com.mongodb.spark._

import IO._
import Interset._
import Ratios._

object ETL {
  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <inter> is a path to interset shape files
        |  <nl> is a path to nl csv files
        |  <pop> is a path to population csv files
        |  <mongo> is a path to mongo, 127.0.0.1:27017/inequality.etl
        """.stripMargin)
      System.exit(1)
    }

    val Array(inter, nl, pop, mongo) = args

    // interactive
    // val Array(inter, nl, pop) = Array("data_example/03_Interset_Pop_Night", "data_example/04_Zonnal_Stats/Night", "data_example/04_Zonnal_Stats/Pop")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Inequality-ETL")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .config("spark.mongodb.output.uri", s"mongodb://$mongo")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext
    implicit val sqlContext : SQLContext = new SQLContext(sc)
    import spark.implicits._

    // go.
    // looking at interset, a couple points seem to be duplicates.
    // val c = spark.sql("select geometry from interset WHERE nl_obs = 52448 AND nl_part = 5023")p.collect()
    // distinct on geometry whener interset is created?
    val intersetDF = readShapeFile(inter).toDF
    intersetDF.createOrReplaceTempView("interset")
    val nightlight = readCsvs(nl)
    nightlight.createOrReplaceTempView("nightlight")
    val population = readCsvs(pop)
    population.createOrReplaceTempView("population")

    val intersetMerged = spark.sql("""
	SELECT * FROM interset i
	CROSS JOIN population p ON p.obs = i.pop_obs AND p.part = i.pop_part
	CROSS JOIN nightlight n ON n.obs = i.nl_obs AND n.part = i.nl_part""")

    val m = ratioAdder(intersetMerged, ratioPairs)
      .drop(nightlight.columns.filter(_ matches "F.+"): _*)
      .drop("obs", "part")

    MongoSpark.save(m.write.mode("overwrite"))
  }
}
