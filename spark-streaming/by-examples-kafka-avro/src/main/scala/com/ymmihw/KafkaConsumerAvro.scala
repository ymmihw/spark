package com.ymmihw

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object KafkaConsumerAvro {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExample.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.16.10.177:9092")
      .option("subscribe", "avro_topic")
      .option("startingOffsets", "earliest") // From starting
      .load()

    /*
     Prints Kafka schema with columns (topic, offset, partition e.t.c)
      */
    df.printSchema()

    /*
    Read schema to convert Avro data to DataFrame
     */

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get(ClassLoader.getSystemResource("person.avsc").toURI)))

    val personDF = df.select(from_avro(col("value"), jsonFormatSchema).as("person"))
      .select("person.*")

    personDF.printSchema()

    /*
    Stream data to Console for testing
     */
    personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}
