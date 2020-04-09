package com.huyong.bigdata.spark.sparksql


import org.apache.spark.sql.SparkSession

/**
  * Created by yonghu on 2020/4/8.
  */
object CaseDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("My Demo 1").getOrCreate()

    val lineRDD = spark.sparkContext.textFile("d:\\\\temp\\\\student.txt").map(_.split(" "))


    val studentRDD = lineRDD.map( x => Student(x(0).toInt, x(1), x(2).toInt))

    import spark.sqlContext.implicits._

    val studentDF = studentRDD.toDF


    studentDF.createOrReplaceTempView("student")

    spark.sql("select * from student").show()

    spark.stop

  }

}



case class Student(stuID: Int, stuName: String, stuAge: Int)
