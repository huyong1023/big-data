package com.huyong.bigdata.spark.streaming.realtime.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util

import com.alibaba.fastjson.JSON
import com.huyong.bigdata.spark.streaming.realtime.bean.Startuplog
import com.huyong.bigdata.spark.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.{ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis




/**
  * Created by yonghu on 2020/4/22.
  */
class DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("", ssc)

    val startuplogStream: DStream[Startuplog] = inputDStream.map { record =>
      val jsonStr: String = record.value()
      val startuplog: Startuplog = JSON.parseObject(jsonStr, classOf[Startuplog])
      val date = new Date(startuplog.ts)
      val dateStr: String = new SimpleDateFormat("yyyy-MM-DD HH:mm").format(date)
      val dateArr: Array[String] = dateStr.split(" ")
      startuplog.logDate = dateArr(0)
      startuplog.logHour = dateArr(1).split(":")(0)
      startuplog.logHourMinute = dateArr(1)

      startuplog
    }

    val filteredDstream: DStream[Startuplog] = startuplogStream.transform{ rdd =>
      println("  ")
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "dau:" + curdate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filteredRDD: RDD[Startuplog] = rdd.filter{ startuplog =>
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startuplog.mid)
      }
      println("")
      filteredRDD
    }

    val groupbyMidDstrem: DStream[(String, Iterable[Startuplog])] = filteredDstream.map(startuplog=>(startuplog.mid, startuplog)).groupByKey()

    val distinctDstream: DStream[Startuplog] = groupbyMidDstrem.flatMap{ case (mid, startulogItr) =>
      startulogItr.take(1)
    }


    distinctDstream.foreachRDD{ rdd=>
      rdd.foreachPartition{ startuplogItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val list: List[Startuplog] = startuplogItr.toList
        for ( startuplog<- list){
          val key = "dau:" + startuplog.logDate
          val value = startuplog.mid
          jedis.sadd(key, value)
          println(startuplog)
        }

        MyEsUtil.indexBulk("", list)
        jedis.close()

      }

    }


    ssc.start()
    ssc.awaitTermination()


  }

}
