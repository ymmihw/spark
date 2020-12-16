package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReduceByKeyLocallyAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReduceByKeyLocallyAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b = a.map(x => (x.length, x))
    val c = b.reduceByKeyLocally(_ + _)
    println(c.mkString(", "))
    val d = b.reduceByKey(_ + _).collect
    println(d.mkString(", "))
    spark.stop()
  }
}
