package com.huyong.bigdata.spark.sparksql

import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by yonghu on 2020/4/8.
  */
object SpecifyingSchema {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").appName("UnderstandingSparkSession").getOrCreate()

    val personRDD = spark.sparkContext.textFile("D:\\\\temp\\\\student.txt").map(_.split(" "))

    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))

    val personDataFrame = spark.createDataFrame(rowRDD, schema)

    personDataFrame.createOrReplaceTempView("t_person")

    val df = spark.sql("select * from t_person order by age desc limit 4")

    df.show()

    spark.stop()

  }

}
