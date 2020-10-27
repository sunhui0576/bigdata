package com.cvcxy.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount {

    def main(args: Array[String]): Unit = {


        // 创建上下文环境
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // source 文件或目录
        val fileRDD: RDD[String] = sc.textFile("input/word.txt")
        // 处理数据
        val words = fileRDD.flatMap(_.split(" ")).cache()
        // TODO 1.分组 求长度
        words.groupBy(word=>word)
          .map { case (word, iter) => {
                (word, iter.size)}}
        // TODO 2.转二元组 reduceByKey
        words.map( word=>(word,1) )
          .reduceByKey(_+_)
        // TODO 3.countByKey
        words.map( word=>(word,1) ).countByKey()
        // TODO 4.转二元组 countByValue
        words.countByValue()
        // TODO aggregate

        // TODO 1, groupBy
        // TODO 2, groupByKey
        // TODO 3, reduceByKey
        // TODO 4, aggregateByKey
        // TODO 5, foldByKey
        // TODO 6, combineByKey
        // TODO 7. countByKey
        // TODO 8. countByValue
        val rdd = sc.makeRDD(List("a", "a", "a", "b", "b"))

        // TODO 9. reduce : (Map, Map) => Map
        val mapRDD = rdd.map(word => Map[String, Int]((word, 1)))
        mapRDD.reduce(( map1, map2 ) => {
                    map1.foldLeft(map2)(
                        ( map, kv ) => {
                            val word = kv._1
                            val count = kv._2
                            map.updated( word, map.getOrElse(word, 0) + count )
                        }
                    )
                }
          )
        // TODO 10. fold
        mapRDD.fold( Map[String, Int]() )(
                    ( map1, map2 ) => {
                        map1.foldLeft(map2)(
                            ( map, kv ) => {
                                val word = kv._1
                                val count = kv._2
                                map.updated( word, map.getOrElse(word, 0) + count )
                            }
                        )
                    }
        )
        // TODO 11. aggregate
        rdd.aggregate(Map[String, Int]())(
            (map, k) => {map.updated( k, map.getOrElse(k, 0) + 1 )},
            ( map1, map2 ) => {
                map1.foldLeft(map2)(
                    ( map, kv ) => {
                        val word = kv._1
                        val count = kv._2
                        map.updated( word, map.getOrElse(word, 0) + count )
                    }
                )
            }
        )
        //关闭连接
        sc.stop()

    }
}
