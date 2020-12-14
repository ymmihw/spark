package com.ymmihw;
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};
object CsvApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CsvApp").setMaster("local[*]")
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val customizedSchema = StructType(Array(StructField("gender", StringType, true), StructField("race", StringType, true), StructField("parentalLevelOfEducation", StringType, true), StructField("lunch", StringType, true), StructField("testPreparationCourse", StringType, true), StructField("mathScore", IntegerType, true), StructField("readingScore", IntegerType, true), StructField("writingScore", IntegerType, true)))
    val pathToFile = "spark-helloworld/StudentsPerformance.csv"
    val DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customizedSchema).load(pathToFile)
    print("We are starting from here...!")
    DF.rdd.cache()
    DF.rdd.foreach(println)
    println(DF.printSchema)
    DF.createOrReplaceTempView("Student")
    sqlContext.sql("SELECT * FROM Student").show()
    sqlContext.sql("SELECT gender, race, parentalLevelOfEducation, mathScore FROM Student WHERE mathScore > 75").show()
    sqlContext.sql("SELECT race, count(race) FROM Student GROUP BY race").show()
    sqlContext.sql("SELECT gender, race, parentalLevelOfEducation, mathScore, readingScore FROM Student").filter("readingScore>90").show()
    sqlContext.sql("SELECT race, parentalLevelOfEducation FROM Student").distinct.show()
    sqlContext.sql("SELECT gender, race, parentalLevelOfEducation, mathScore, readingScore FROM Student WHERE mathScore> 75 and readingScore>90").show()
    sqlContext.sql("SELECT gender, race, parentalLevelOfEducation, mathScore, readingScore FROM Student").dropDuplicates().show()
    println("We have finished here...!")
    spark.stop()
  }
}