package com.ymmihw

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()


    val columns = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)


    //From RDD (USING toDF())
    {
      import spark.implicits._

      val dfFromRDD1 = rdd.toDF("language", "users")
      dfFromRDD1.printSchema()
    }
    //From RDD (USING createDataFrame)
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns: _*)
    dfFromRDD2.printSchema()
    //From RDD (USING createDataFrame and Adding schema using StructType)
    //convert RDD[T] to RDD[Row]
    val schema = StructType(Array(StructField("language", StringType, true),
      StructField("users_count", StringType, true)))

    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
    val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)


    //From Data (USING toDF())
    {
      import spark.implicits._
      val dfFromData1 = data.toDF()
    }
    //From Data (USING createDataFrame)
    var dfFromData2 = spark.createDataFrame(data).toDF(columns: _*)

    //From Data (USING createDataFrame and Adding schema using StructType)
    import scala.collection.JavaConverters._

    val rowData = data.map(attributes => Row(attributes._1, attributes._2))
    var dfFromData3 = spark.createDataFrame(rowData.asJava, schema)
    dfFromData3.printSchema()
  }
}