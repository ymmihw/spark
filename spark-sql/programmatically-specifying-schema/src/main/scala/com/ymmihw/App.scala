package com.ymmihw

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val employee = spark.sparkContext.textFile(App.getClass.getClassLoader.getResource("employee.txt").toString);
    val schema = StructType(Array(
      StructField("id", IntegerType, true)
      , StructField("name", StringType, true)
      , StructField("age", IntegerType, true)
    ))
    val rdd = employee.map(_.split(",")).map(e ⇒ Row(e(0).trim.toInt, e(1), e(2).trim.toInt))
    val dfs = spark.createDataFrame(rdd, schema)
    dfs.createTempView("employee")
    val allRecords = spark.sql("SELECT * FROM employee")
    println("=========allRecords=========")
    allRecords.show()

    val ageFilter = spark.sql("SELECT * FROM employee WHERE age >= 20 AND age <= 35")
    println("=========ageFilter=========")
    ageFilter.show()

    import spark.implicits._

    println("=========Fetch ID values from ageFilter DataFrame using column index=========")
    ageFilter.map(t => "ID: " + t(0)).collect().foreach(println)
  }
}
