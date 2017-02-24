package edu.upf.inequality.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Ratios {
  val ratioPairs = Seq(
    ("gini90_F10","F101992_sum","90ag_sum"),
    ("gini95_F12","F121995_sum","95ag_sum"),
    ("gini00_F14","F142000_sum","00ag_sum"),
    ("gini00_F15","F152000_sum","00ag_sum"),
    ("gini05_F15","F152005_sum","05ag_sum"),
    ("gini05_F16","F162005_sum","05ag_sum"),
    ("gini10_F18","F182010_sum","10ag_sum")
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
