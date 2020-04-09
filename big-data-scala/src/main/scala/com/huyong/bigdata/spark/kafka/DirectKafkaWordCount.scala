package com.huyong.bigdata.spark.kafka

import io.netty.handler.codec.string.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yonghu on 2020/4/9.
  */
class DirectKafkaWordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFlumeNGWrodCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))


    val topics = Set("mydemo1")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.157.81:9092")

//    val kafkaStrem = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//
//    val lineDStream = kafkaStrem.map(e => {
//      new String(e.toString())
//    })
//
//    lineDStream.print()
//
//    ssc.start()
//    ssc.awaitTermination()

  }

}
