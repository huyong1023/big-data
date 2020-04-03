package com.huyong.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yonghu on 2020/4/3.
  */
object SparkWordCountWithScala {

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setMaster("local")
    conf.setAppName("WorkCount")

    val sc = new SparkContext(conf)
    var file: RDD[String] = sc.textFile("/Users/yonghu/Downloads/hello.txt")

    val word: RDD[String] = file.flatMap(_.split(","))

    var wordOne: RDD[(String, Int)] = word.map((_,1))

    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)

    val sortRdd: RDD[(String, Int)] = wordCount.sortBy(tuple => tuple._2,false)

    sortRdd.saveAsTextFile("/Users/yonghu/Downloads/result.txt")

    sc.stop()

  }

}
