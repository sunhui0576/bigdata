package com.cvcxy.realtime.app

import com.alibaba.fastjson.JSON
import com.cvcxy.common.Constant
import com.cvcxy.realtime.bean.{AlertInfo, EventLog}
import com.cvcxy.realtime.util.MyKafkaUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import java.util

import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks._
/**
 * 需求：
 * 同一设备:按设备id分组
 * 5分钟内
 * 三次及以上用不同账号登录
 * 领取优惠劵，
 * 并且在登录到领劵过程中没有浏览商品。
 * 同时达到以上要求则产生一条预警日志。
 * 同一设备，每分钟只记录一次预警。
 */
object AlertApp extends BaseApp {
  override def run(ssc: StreamingContext): Unit = {
    // 读取kafka数据
    val eventlogStream = MyKafkaUtil.getKafkaStream(ssc,Constant.EVENT_TOPIC)
      //数据转样例类
      .map(log=>JSON.parseObject(log,classOf[EventLog]))
      //开窗
      .window(Minutes(5),Seconds(6))
    //转二元组，分组聚合
    val eventLogGroupedStream: DStream[(String, Iterable[EventLog])] = eventlogStream.map(event => (event.mid,event)).groupByKey()
    val alertInfoStream = eventLogGroupedStream.map {
      case (mid, eventLogIt) => {
        val uidSet = new util.HashSet[String]()
        val itemSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        var isClickItem = true
        breakable {
          eventLogIt.foreach(log => {
            eventList.add(log.eventId)
            log.eventId match {
              case "coupon" =>
                uidSet.add(log.uid)
              case "clickItem" =>
                isClickItem = false
                break
              case _ =>
            }
          })
        }
        (isClickItem && uidSet.size() > 3, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
      }
    }
    alertInfoStream.print(1000)
  }
}
