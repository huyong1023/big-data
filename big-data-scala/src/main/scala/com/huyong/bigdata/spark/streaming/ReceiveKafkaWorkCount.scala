package com.huyong.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * Created by yonghu on 2020/4/9.
  */
class ReceiveKafkaWorkCount {
  def main(args: Array[String]) {
    if(args.length < 4 ){
      System.err.println("Usage: KafkaWordCount <zkQuorun> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("kafkaWordcOUNT").setMaster("loal[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    ssc.start()
    ssc.awaitTermination()

  }

}
