package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ForeachAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ForeachAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val a = sc.parallelize(List("V1", "V2", "V3", "U1", "U2"), 2)
    a.foreach(e => println(e))
    spark.stop()
  }
}
