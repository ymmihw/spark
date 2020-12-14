package com.ymmihw.transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MapTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val a = spark.sparkContext.parallelize(List("V1", "V2", "V3"), 3)
    val b = a.map(s => s.substring(0, s.length - 1) + "'" + s.substring(s.length - 1)).collect()
    println(b.mkString(", "))
    spark.stop()
  }
}
