package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TakeSampleTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TakeSampleTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V2", "U1", "U2", "U3", "U4"), 2)
    val c = a.takeSample(false, 1, 9)
    println(c.mkString(", "))
    spark.stop()
  }
}
