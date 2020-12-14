package com.ymmihw.transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object GlomTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GlomTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V2", "V3", "U1", "U2"), 2).glom.collect
    println(a.mkString(", "))
    spark.stop()
  }
}
