package com.ymmihw.transformation

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession

object PartitionByTransformation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PartitionByTransformation").setMaster("local[*]")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val a = spark.sparkContext.parallelize(List(("V1", 1), ("V2", 2), ("V3", 4), ("V1", 3), ("W3", 1),
      ("U1", 2), ("U1", 1)), 2)
    val b = a.partitionBy(new HashPartitioner(3)).collect
    println(b.mkString(", "))
    spark.stop()
  }
}
