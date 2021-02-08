package com.ymmihw

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct}
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object KafkaProduceAvro {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample.com")
      .getOrCreate()

    /*
    Disable logging as it writes too much log
     */
    spark.sparkContext.setLogLevel("ERROR")

    /*
    This consumes JSON data from Kafka
     */
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.16.10.177:9092")
      .option("subscribe", "json_topic")
      .option("startingOffsets", "earliest") // From starting
      .load()

    /*
     Prints Kafka schema with columns (topic, offset, partition e.t.c)
      */
    df.printSchema()

    val schema = new StructType()
      .add("id", IntegerType)
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("dob_year", IntegerType)
      .add("dob_month", IntegerType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    /*
    Converts JSON string to DataFrame
     */
    val personDF = df.selectExpr("CAST(value AS STRING)") // First convert binary to string
      .select(from_json(col("value"), schema).as("data"))


    personDF.printSchema()

    personDF.select(to_avro(struct("data.*")) as "value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "172.16.10.177:9092")
      .option("topic", "avro_topic")
      .option("checkpointLocation", "c:/tmp")
      .start()
      .awaitTermination()
  }

}
