package com.cvcxy.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class EventLog(mid:String,
                    uid:String,
                    appId:String,
                    area:String,
                    os:String,
                    logType:String,
                    eventId:String,
                    pageId:String,
                    nextPageId:String,
                    itemId:String,
                    ts:Long,
                   //当前日期
                    var logDate:String=null,
                   //当前小时
                    var logHour:String=null){
  private val date=new Date()
  logDate=new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour=new SimpleDateFormat("HH").format(date)
}
