package com.ymmihw

import org.apache.spark.sql.SparkSession

object SampleApp extends App {
  val spark = SparkSession.builder().master("local[1]").appName("SparkByExample")
    .getOrCreate();
  val df = spark.range(100)
  println(df.sample(0.1).collect().mkString(","))

  println(df.sample(0.1, 123).collect().mkString(","))
  //Output: 36,37,41,43,56,66,69,75,83
  println(df.sample(0.1, 123).collect().mkString(","))
  //Output: 36,37,41,43,56,66,69,75,83
  println(df.sample(0.1, 456).collect().mkString(","))
  //Output: 19,21,42,48,49,50,75,80

  println(df.sample(true, 0.3, 123).collect().mkString(",")) //with Duplicates
  //Output: 0,5,9,11,14,14,16,17,21,29,33,41,42,52,52,54,58,65,65,71,76,79,85,96
  println(df.sample(0.3, 123).collect().mkString(",")) // No duplicates
  //Output: 0,4,17,19,24,25,26,36,37,41,43,44,53,56,66,68,69,70,71,75,76,78,83,84,88,94,96,97,98

  println(df.stat.sampleBy("id", Map("" -> 0.1), 123).collect().mkString(","))
  //Output: 6,13,17,19,78

  val rdd = spark.sparkContext.range(0, 100)
  println(rdd.sample(false, 0.1, 0).collect().mkString(","))
  //Output: 1,20,29,42,53,62,63,70,82,87
  println(rdd.sample(true, 0.3, 123).collect().mkString(","))
  //Output: 1,4,21,30,32,32,32,33,42,45,46,52,53,54,55,58,58,66,66,68,70,70,74,86,92,96,98,99

  println(rdd.takeSample(false, 10, 0).mkString(","))
  //Output: 62,30,27,29,21,16,86,7,20,91
  println(rdd.takeSample(true, 30, 123).mkString(","))
  //Output: 85,49,61,16,90,5,33,98,89,38,89,29,5,48,24,60,41,33,13,40,14,33,56,95,40,48,61,36,82,9
}