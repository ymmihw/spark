package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TopAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ForeachAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(Array(6, 9, 4, 7, 5, 8), 2)
    val b = a.top(2)
    println(b.mkString(", "))
    spark.stop()
  }
}
