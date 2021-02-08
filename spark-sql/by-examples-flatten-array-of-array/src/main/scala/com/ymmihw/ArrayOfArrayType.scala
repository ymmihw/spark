package com.ymmihw

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.flatten
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ArrayOfArrayType extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayArrayData = Seq(
    Row("James", List(List("Java", "Scala", "C++"), List("Spark", "Java"))),
    Row("Michael", List(List("Spark", "Java", "C++"), List("Spark", "Java"))),
    Row("Robert", List(List("CSharp", "VB"), List("Spark", "Python")))
  )

  val arrayArraySchema = new StructType().add("name", StringType)
    .add("subjects", ArrayType(ArrayType(StringType)))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema)
  df.printSchema()
  df.show(false)

  //Convert Array of Array into Single array
  df.select(col("name"), flatten(col("subjects"))).show(false)

}