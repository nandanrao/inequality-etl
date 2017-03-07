package edu.upf.inequality.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Ratios {

  val datasets = Map(
    "gpw_v3" -> Seq(
      ("gini90_F10","F101992_sum","90ag_sum"),
      ("gini95_F12","F121995_sum","95ag_sum"),
      ("gini00_F14","F142000_sum","00ag_sum"),
      ("gini00_F15","F152000_sum","00ag_sum"),
      ("gini05_F15","F152005_sum","05ag_sum"),
      ("gini05_F16","F162005_sum","05ag_sum"),
      ("gini10_F18","F182010_sum","10ag_sum")
    ),
    "landscan" -> Seq(
      ("gini00_F14", "F142000_sum", "pop2000_sum"),
      ("gini01_F14", "F142001_sum", "pop2001_sum"),
      ("gini02_F14", "F142002_sum", "pop2002_sum"),
      ("gini03_F14", "F142003_sum", "pop2003_sum"),
      ("gini00_F15", "F152000_sum", "pop2000_sum"),
      ("gini01_F15", "F152001_sum", "pop2001_sum"),
      ("gini02_F15", "F152002_sum", "pop2002_sum"),
      ("gini03_F15", "F152003_sum", "pop2003_sum"),
      ("gini04_F15", "F152004_sum", "pop2004_sum"),
      ("gini05_F15", "F152005_sum", "pop2005_sum"),
      ("gini06_F15", "F152006_sum", "pop2006_sum"),
      ("gini07_F15", "F152007_sum", "pop2007_sum"),
      ("gini04_F16", "F162004_sum", "pop2004_sum"),
      ("gini05_F16", "F162005_sum", "pop2005_sum"),
      ("gini06_F16", "F162006_sum", "pop2006_sum"),
      ("gini07_F16", "F162007_sum", "pop2007_sum"),
      ("gini08_F16", "F162008_sum", "pop2008_sum"),
      ("gini09_F16", "F162009_sum", "pop2009_sum"),
      ("gini10_F18", "F182010_sum", "pop2010_sum"),
      ("gini11_F18", "F182011_sum", "pop2011_sum"),
      ("gini12_F18", "F182012_sum", "pop2012_sum"),
      ("gini13_F18", "F182013_sum", "pop2013_sum")
    )
  )

  def ratioAdder(df:DataFrame, list:Seq[(String,String,String)])(implicit spark: SparkSession) : DataFrame = {
    import spark.implicits._
    val u = udf(calcRatio _)
    list.foldLeft(df)((df, t) => {
      df.withColumn(t._1, u('int_area, Symbol(t._2), 'nl_area, Symbol(t._3), 'pop_area))
    })
  }

  def calcRatio(area:Double, nl:Double, nlArea:Double, pop:Double, popArea:Double) : Double = {
    nl * area/nlArea / pop*area / popArea
  }
}
