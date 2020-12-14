package com.ymmihw.transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlatMapTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1,V2,V3", "U1,U2", "M1,M2"), 3).flatMap(s => {
      val ss = s.split(",")
      for (i <- 0 to ss.length - 1) {
        val str = ss(i)
        ss(i) = str.substring(0, str.length - 1) + "'" + str.substring(str.length - 1)
      }
      ss
    }).collect
    println(a.mkString(", "))
    spark.stop()
  }
}
