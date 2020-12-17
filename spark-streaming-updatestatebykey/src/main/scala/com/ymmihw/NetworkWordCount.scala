package com.ymmihw

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("/checkpoint/NetworkWordCount");
    val lines = ssc.socketTextStream("172.16.10.177", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.updateStateByKey[Int]((values: Seq[Int], state: Option[Int]) => {
      var updatedValue = 0
      if (state.isDefined) {
        updatedValue = state.get
      }
      for (value <- values) {
        updatedValue += value
      }
      Some(updatedValue)
    }
    )

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}
