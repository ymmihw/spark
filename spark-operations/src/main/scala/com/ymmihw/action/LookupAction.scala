package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LookupAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LookupAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val x = sc.parallelize(List(("V1", 1), ("V2", 1), ("V3", 1), ("U1", 1), ("U2", 1)), 2)
    val y = x.lookup("V1")
    println(y.mkString(", "))
    spark.stop()
  }
}
