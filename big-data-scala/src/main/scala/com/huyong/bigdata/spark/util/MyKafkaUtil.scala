package com.huyong.bigdata.spark.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Created by yonghu on 2020/4/22.
  */
object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  val kafkaParam = Map(
    "" -> broker_list
  )


  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }
}
