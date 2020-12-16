package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CollectAsMapAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CollectAsMapAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val x = sc.parallelize(List(("V1", 1), ("V2", 3), ("V3", 8), ("U1", 3), ("U2", 5)), 2)
    val y = x.collectAsMap
    println(y.mkString(", "))
    spark.stop()
  }
}
