package com.huyong.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yonghu on 2020/4/22.
  */
object StreamWordCount {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Stream WordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lineStreams = ssc.socketTextStream("localhost", 9999)

    val wordStreams = lineStreams.flatMap(_.split(" "))

    val wordAndOneStreams = wordStreams.map((_, 1))

    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    wordAndCountStreams.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
