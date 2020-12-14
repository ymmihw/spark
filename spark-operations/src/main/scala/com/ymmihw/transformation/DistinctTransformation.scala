package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DistinctTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DistinctTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V1", "V3", "U1", "U1", "U1", "U2", "U2"), 2)
    val b = a.distinct.collect
    println(b.mkString(", "))
    spark.stop()
  }
}
