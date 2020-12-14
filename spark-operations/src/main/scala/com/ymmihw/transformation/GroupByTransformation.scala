package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GroupByTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupByTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V2", "U1", "W2", "U2", "V2", "W1"), 3)
    var c = a.groupBy(s => s.substring(0, s.length - 1)).collect
    println(c.mkString(", "))
    spark.stop()
  }
}
