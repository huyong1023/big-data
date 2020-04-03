package com.huyong.bigdata

/**
  * Created by yonghu on 2020/4/2.
  */
object MethodAndFunctionDemo {

  def m1(f:(Int, Int) => Int ) : Int = {
    f(2, 6)
  }


  var f1 = (x:Int, y:Int ) => x + y

  var f2 = (m:Int, n :Int ) => m * n

  def main(args: Array[String]): Unit = {
    var r1 = m1(f1)
    println(r1)

    var r2 = m1(f2)
    println(r2)
  }


}
