package com.ymmihw

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateAction").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val dfs = spark.read.json(App.getClass.getClassLoader.getResource("employee.json").toString)
    println("All -----------------------------")
    dfs.show()
    println("Schema -----------------------------")
    dfs.printSchema()
    println("name -----------------------------")
    dfs.select("name").show()
    println("age > 23 -----------------------------")
    dfs.filter(dfs("age") > 23).show()
    println("group by age -----------------------------")
    dfs.groupBy("age").count().show()
  }
}
