package com.cvcxy.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object MyKafkaUtil {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> ConfigUtil.getProperty("config.properties", "kafka.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> ConfigUtil.getProperty("config.properties", "group.id"),
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(ssc:StreamingContext,topic:String)={
    KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](Set(topic),kafkaParams)
    ).map(_.value())
  }
}
