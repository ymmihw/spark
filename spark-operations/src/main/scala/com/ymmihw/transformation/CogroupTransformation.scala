package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CogroupTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CogroupTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List(("V1", 1), ("V2", 2), ("U1", 1), ("U2", 2), ("U5", 5)), 2)
    val b = sc.parallelize(List(("V1", 1), ("V8", 2), ("U1", 2), ("U5", 1)), 2)
    val c = a.cogroup(b).collect
    println(c.mkString(", "))
    spark.stop()
  }
}
