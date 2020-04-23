package com.huyong.bigdata.spark.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * Created by yonghu on 2020/4/22.
  */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
  }

  def load(propertieName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}
