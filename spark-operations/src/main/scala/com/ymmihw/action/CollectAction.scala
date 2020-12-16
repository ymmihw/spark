package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CollectAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CollectAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val x = sc.parallelize(List("V1", "V2", "V3", "U1", "U2"), 2)
    val y = x.collect
    println(y.mkString(", "))
    spark.stop()
  }
}
