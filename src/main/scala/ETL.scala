package edu.upf.inequality.etl

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


import IO._
import Interset._

object ETL {
  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <inter> is a path to interset shape files
        |  <nl> is a path to nl csv files
        |  <pop> is a path to population csv files
        """.stripMargin)
      System.exit(1)
    }

    val Array(inter, nl, pop) = args

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Inequality-ETL")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext
    implicit val sqlContext : SQLContext = new SQLContext(sc)

    // go.
    val shapefilesRDD = readShapeFile(inter)
    val intersetDF = spark.createDataFrame(shapefilesRDD, IntersetSchema)
    intersetDF.createOrReplaceTempView("interset")
    val nightlight = readCsvs(nl)
    nightlight.createOrReplaceTempView("nightlight")
    val population = readCsvs(pop)
    population.createOrReplaceTempView("population")

    val intersetMerged = spark.sql("""
	SELECT * FROM interset i
	JOIN population p ON p.obs = i.pop_obs AND p.part = i.pop_part
	JOIN nightlight n ON n.obs = i.nl_obs AND n.part = i.nl_part""")

    intersetMerged.printSchema()
  }
}
