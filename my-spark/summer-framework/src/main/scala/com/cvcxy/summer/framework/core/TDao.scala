package com.cvcxy.summer.framework.core

import java.util.Properties

import com.cvcxy.summer.framework.util.PropertiesUtil
import com.cvcxy.summer.framework.util.{EnvUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

//数据源：从本地文件，网络端口，其他框架获取数据
trait TDao {
    // spark读取本地文件
    def readFile(path:String) = {
        EnvUtil.getSCEnv().textFile(path)
    }

    /**
      * spark streaming从Kafka中读取数据
      */
    def readKafka() = {

        val brokerList = PropertiesUtil.getValue("kafka.broker.list")
        val groupid = PropertiesUtil.getValue("kafka.group.id")
        val topic = PropertiesUtil.getValue("kafka.topic")
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
            ConsumerConfig.GROUP_ID_CONFIG -> groupid,
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            KafkaUtils.createDirectStream[String, String](
                EnvUtil.getSSCEnv(),
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaPara)
            )

        kafkaDStream.map(record => record.value())
    }

    /**
      * spark streaming向kafka中发送数据
      */
    def writeToKakfa( datas:()=>Seq[String] ): Unit = {
        val broker: String = PropertiesUtil.getValue("kafka.broker.list")
        val topic = PropertiesUtil.getValue("kafka.topic")

        // 创建Kafka消费者
        // 创建配置对象
        val prop = new Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        // 根据配置创建Kafka生产者
        val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

        while (true) {
            // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
            for (line <- datas()) {
                kafkaProducer.send(new ProducerRecord[String, String](topic, line))
                println(line)
            }
            Thread.sleep(2000)
        }
    }
}
