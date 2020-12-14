package com.ymmihw.transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CartesianTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CartesianTransformaion").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V2", "U1", "U2"), 2)
    val b = sc.parallelize(List("W1", "W2", "Q5"), 2)
    var c = a.cartesian(b).collect
    println(c.mkString(", "))
    spark.stop()
  }
}
