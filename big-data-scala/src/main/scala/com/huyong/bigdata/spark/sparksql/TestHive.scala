package com.huyong.bigdata.spark.sparksql


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yonghu on 2020/4/9.
  */
class TestHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("loal").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("select * from myhive.student").show()
  }
}
