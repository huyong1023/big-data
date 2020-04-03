package com.huyong.bigdata.classandobject


import java.io._



/**
  * Created by yonghu on 2020/4/2.
  */

class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc


  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println("x point :" + x)
    println("y point :" + y)

  }
}

//
//class Location(override val xc: Int, override val yc: Int, val zc: Int) extends Point(xc,yc){
//  var z: Int = zc
//
//  def move(dx: Int, dy: Int, dz: Int) {
//    x = x + dx
//    y = y + dy
//    z = z + dz
//    println("x point :" + x)
//    println("y point :" + y)
//    println("z point :" + z)
//  }
//}


object Test {

  def main(args: Array[String]) {
    val pt = new Point(10, 20);

    pt.move(10, 10)
  }

}
