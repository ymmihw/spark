package com.ymmihw.transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlatMapTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(1 to 10, 5).flatMap(1 to _).collect
    val b = sc.parallelize(List(1, 2, 3), 2).flatMap(x => List(x, x, x)).collect
    println(a.mkString(", "))
    println(b.mkString(", "))
    spark.stop()
  }
}
