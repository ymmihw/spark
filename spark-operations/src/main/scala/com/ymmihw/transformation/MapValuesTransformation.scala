package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MapValuesTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapValuesTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val a = spark.sparkContext.parallelize(List(("V1", 1), ("V2", 2), ("V3", 4), ("U1", 3), ("U2", 4)), 2)
    val b = a.mapValues(a => a + 2).collect()
    println(b.mkString(", "))
    spark.stop()
  }
}
