package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SampleTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SampleTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V2", "U1", "U2", "U3", "U4"), 2)
    val c = a.sample(false, 0.5, 9).collect
    println(c.mkString(", "))
    spark.stop()
  }
}
