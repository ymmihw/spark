package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReduceByKeyTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReduceByKeyTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val a = spark.sparkContext.parallelize(List(("V1", 2), ("V1", 1), ("V2", 2), ("V3", 1), ("U1", 2), ("U1", 1), ("U1", 3)), 2)
    val b = a.reduceByKey((a, b) => a + b).collect()
    println(b.mkString(", "))
    spark.stop()
  }
}
