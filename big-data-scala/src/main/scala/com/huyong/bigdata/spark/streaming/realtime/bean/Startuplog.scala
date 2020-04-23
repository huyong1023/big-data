package com.huyong.bigdata.spark.streaming.realtime.bean

/**
  * Created by yonghu on 2020/4/22.
  */
case class Startuplog (mid:String,
                       uid:String,
                       appid:String,
                       area:String,
                       os:String,
                       ch:String,
                       logType:String,
                       vs:String,
                       var logDate:String,
                       var logHour:String,
                       var logHourMinute:String,
                       var ts:Long
                      ){

}