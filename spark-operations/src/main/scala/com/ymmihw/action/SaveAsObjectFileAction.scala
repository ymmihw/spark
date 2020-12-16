package com.ymmihw.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaveAsObjectFileAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SaveAsOSbjectFileAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val x = sc.parallelize(1 to 100, 3)
    x.saveAsObjectFile("objFile")
    val y = sc.objectFile[Int]("objFile")
    spark.stop()
  }
}
