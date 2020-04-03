package com.huyong.bigdata

/**
  * Created by yonghu on 2020/4/2.
  */
object TestMap {

  def ttt(f:Int => Int):Unit = {
    var r = f(10)
    println(r)
  }

  val f0 = (x : Int) => x * x


  def m0(x:Int) : Int = {
    x * 10
  }

  val f1 = m0 _

  def main(args: Array[String]): Unit = {
    ttt(f0)

    ttt(m0 _);

    ttt(m0)


    ttt(x => m0(x))
  }

}
