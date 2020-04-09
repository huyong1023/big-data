package com.huyong.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by yonghu on 2020/4/3.
  */
object SparkWordCountWithScala {


  private val conf: SparkConf = new SparkConf().setAppName("TestTransformation").setMaster("local")
  private val sc = new SparkContext(conf)


  def main(args : Array[String]): Unit = {
    var file: RDD[String] = sc.textFile("/Users/yonghu/Downloads/hello.txt")

    val word: RDD[String] = file.flatMap(_.split(","))

    var wordOne: RDD[(String, Int)] = word.map((_,1))

    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)

    val sortRdd: RDD[(String, Int)] = wordCount.sortBy(tuple => tuple._2,false)

    sortRdd.saveAsTextFile("/Users/yonghu/Downloads/result.txt")

    sc.stop()

  }


  def map(): Unit = {
    val list = List("张无忌", "赵敏", "周芷若")
    var listRDD = sc.parallelize(list)
    var nameRDD = listRDD.map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }


  def flatMap(): Unit ={
    val list = List("张无忌 赵敏","宋青书 周芷若")
    val listRDD = sc.parallelize(list)

    var nameRDD = listRDD.flatMap(line => line.split(" ").map(name => "Hello " + name))
    nameRDD.foreach(name => println(name))
  }


  def mapParations(): Unit ={
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list, 2)

    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext){
        newList.append("hello " + iterator.next())
      }
      newList.toIterator
    }).foreach(name => println(name))
  }


  def mapPartitionsWithIndex() : Unit = {
    val list = List(1,2,3,4,5,6,7,8)
    sc.parallelize(list).mapPartitionsWithIndex((index, iterator) => {
      val listBuffer:ListBuffer[String] = new ListBuffer
      while (iterator.hasNext){
        listBuffer.append(index+"_"+iterator.next())
      }
      listBuffer.iterator
    }, true)
      .foreach(println(_))
  }


  def reduce(): Unit ={
    val list = List(1,2,3,4,5,6)
    val listRDD = sc.parallelize(list)

    val result = listRDD.reduce((x,y) => x+y)
    println(result)
  }


  def reduceByKey(): Unit = {

    val list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77))
    val mapRDD = sc.parallelize(list)

    val resultRDD = mapRDD.reduceByKey(_+_)
    resultRDD.foreach(tuple => println("门派: " + tuple._1 + "->" + tuple._2))
  }


  def union(): Unit ={
    val list1 = List(1,2,3,4)
    val list2 = List(3,4,5,6)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    rdd1.union(rdd2).foreach(println(_))
  }


  def groupByKey(): Unit = {
    val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
    val listRDD = sc.parallelize(list)
    val groupByKeyRDD = listRDD.groupByKey()
    groupByKeyRDD.foreach(t => {
      val menpai = t._1
      val iterator = t._2.iterator
      var people = ""
      while (iterator.hasNext) people = people + iterator.next() + " "
      println("门派:" + menpai + "人员:" + people)
    })
  }



  def join(): Unit = {
    val list1 = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
    val list2 = List((1, 99), (2, 98), (3, 97))
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)

    val joinRDD = list1RDD.join(list2RDD)
    joinRDD.foreach(t => println("学号:" + t._1 + " 姓名:" + t._2._1 + " 成绩:" + t._2._2))
   }


  def sample(): Unit = {
    val list = 1 to 100
    val listRDD = sc.parallelize(list)
    listRDD.sample(false, 0.1, 0).foreach(num => print(num + ""))
  }



  def cartesian(): Unit = {
    val list1 = List("A", "B")
    val list2 = List(1,2,3)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.cartesian(list2RDD).foreach(t => println(t._1 + "->"+t._2))
  }


  def filter(): Unit = {
    val list = List(1,2,3,4,5,6,7,8,9,10)
    val listRDD = sc.parallelize(list)
    listRDD.filter(num => num %2 ==0).foreach(print(_))
  }


  def discinct(): Unit = {
    val list = List(1,2,3,4,5)
    sc.parallelize(list).distinct().foreach(println(_))
  }



  def interesction(): Unit = {
    val list1 = List(1,2,3,4)
    val list2 = List(3,4,5,6)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.intersection(list2RDD).foreach(println(_))
  }







  def coalesce(): Unit = {
    val list = List(1,2,3,4,5,6,7,8,9)
    sc.parallelize(list, 3).coalesce(1).foreach(println(_))
  }



  def replication(): Unit = {
    val list = List(1,2,3,4)
    val listRDD = sc.parallelize(list, 1)
    listRDD.repartition(2).foreach(println(_))
  }



  def repartitionAndSortWithPartitions(): Unit ={
    val list = List(1, 4, 55, 66, 33, 48, 23)
    val listRDD = sc.parallelize(list, 1)
    listRDD.map(num => (num, num))
      .repartitionAndSortWithinPartitions(new HashPartitioner(2))
      .mapPartitionsWithIndex((index, iterator) => {
      val listBuffer: ListBuffer[String] = new ListBuffer
      while (iterator.hasNext) {
        listBuffer.append(index + "_" + iterator.next())
      }
      listBuffer.iterator
    }, false)
      .foreach(println(_))
  }



  def cogroup(): Unit = {
    val list1 = List((1, "www"), (2, "bbs"))
    val list2 = List((1, "cnblog"), (2, "cnblog"), (3, "very"))
    val list3 = List((1, "com"), (2, "com"), (3, "good"))

    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    val list3RDD = sc.parallelize(list3)


    list1RDD.cogroup(list2RDD, list3RDD).foreach(tuple =>
      println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2 + " " + tuple._2._3)
    )
  }



  def sortByKey(): Unit ={
    val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
    sc.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
  }


  def aggregateByKey(): Unit ={
    val list = List("you,jump", "i,jump")
    sc.parallelize(list)
      .flatMap(_.split(","))
      .map((_, 1))
      .aggregateByKey(0)(_+_,_+_)
      .foreach(tuple =>println(tuple._1+"->"+tuple._2))
  }

}
