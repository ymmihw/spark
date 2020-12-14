package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MapPartitionsTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapPartitionsTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(1 to 6, 2).filter(e => e >= 3).collect
    println(a.mkString(", "))
    spark.stop()
  }
}
