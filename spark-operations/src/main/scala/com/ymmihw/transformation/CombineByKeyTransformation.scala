package com.ymmihw.transformation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CombineByKeyTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CombineByKeyTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val a = spark.sparkContext.parallelize(List(("V1", 2), ("V1", 1), ("V2", 2), ("V3", 1), ("U1", 2), ("U1", 1), ("U1", 3), ("V1", 3)), 2)
    val b = a.combineByKey(List(_), (x: List[Int], y: Int) => y :: x, (x: List[Int], y: List[Int]) => x ::: y).collect()
    println(b.mkString(", "))
    spark.stop()
  }
}
