package com.huyong.bigdata.spark.streaming

import java.util
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by yonghu on 2020/4/9.
  */
object KafkaWordCountProducer {
  def main(args: Array[String]) {
    val topic = "test"
    val brokers = "node01:9092,node02:9092,node03:9092"
    val messagesPerSec=1 //每秒发送几条信息
    val wordsPerMessage =4 //一条信息包括多少个单词
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
        println(message)
      }
      Thread.sleep(1000)
    }
  }
}
