package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AggregateAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val x = sc.parallelize(List(("V1", 1), ("V3", 2), ("U1", 2), ("U2", 2), ("U3", 4), ("U4", 1)), 2)
    val y = x.aggregate("V0", 0)((a, b) => (a._1 + "@" + b._1, a._2 + b._2), (a, b) => (a._1 + "@" + b._1, a._2 + b._2))
    println(y)
    spark.stop()
  }
}
