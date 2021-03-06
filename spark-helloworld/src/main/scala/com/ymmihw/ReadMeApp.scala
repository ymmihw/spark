package com.ymmihw;

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession;

object ReadMeApp {
  def main(args: Array[String]) {
//    val logFile = "spark-helloworld/README.md" // Should be some file on your system
    val logFile = CsvApp.getClass.getClassLoader.getResource("README.md").toString
    val conf = new SparkConf().setAppName("ReadMeApp").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}