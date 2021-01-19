package com.ymmihw

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val rdd = spark.sparkContext.textFile(App.getClass.getClassLoader.getResource("employee.txt").toString)
      .map(_.split(","))
      .map(e ⇒ Employee(e(0).trim.toInt, e(1), e(2).trim.toInt))
    val dfs = spark.createDataFrame(rdd)
    dfs.createTempView("employee")
    val allrecords = spark.sql("SELECT * FROM employee")
    println("=========allrecords=========")
    allrecords.show()

    val agefilter = spark.sql("SELECT * FROM employee WHERE age >= 20 AND age <= 35")
    println("=========agefilter=========")
    agefilter.show()

    import spark.implicits._

    println("=========Fetch ID values from agefilter DataFrame using column index=========")
    agefilter.map(t => "ID: " + t(0)).collect().foreach(println)
  }
}
