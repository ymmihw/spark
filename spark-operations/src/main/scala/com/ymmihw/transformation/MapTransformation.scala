package com.ymmihw.transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MapTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val a = spark.sparkContext.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.map(_.length)
    val c = a.zip(b)
    var d = c.collect()
    println(d.mkString(", "))
    spark.stop()
  }
}
