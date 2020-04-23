package com.huyong.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Subscribe, _}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yonghu on 2020/4/9.
  */
object DirectKafkaWordCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaWordCount")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.streaming.backpressure.enabled", "false")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.kafka.maxRetries", "3")

    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(1))


    val offsets: Map[TopicPartition, Long] = Map(new TopicPartition("topic01", 0) -> 2495038L)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

/*

    /** 1、这种订阅会读取所有的partition数据 但是可以指定某些partition的offset */
    val stream1: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams, offsets)
    )


    stream1.foreachRDD(lineRDD => {
      if (!lineRDD.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = lineRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        lineRDD.foreachPartition(iter => {
          iter.foreach(record => {
            println("partition = " + record.partition(), " key = " + record.key(), " value = " + record.value(), " offset = " + record.offset())
          })
        })
        stream1.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
*/


    streamingContext.start()
    streamingContext.awaitTermination()









  }

}
