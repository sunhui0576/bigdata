package com.cvcxy.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Dependencies {

    def main(args: Array[String]): Unit = {

        // Spark 依赖关系
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // TODO 血缘关系 依赖关系
        val rdd = sc.makeRDD(List(
            "hello scala", "hello spark"
        ))
        println("--------血缘关系----------")
        // new ParallelCollectionRDD
        println(rdd.toDebugString)
        println("-------依赖关系-----------")
        // List
        println(rdd.dependencies)
        val wordRDD = rdd.flatMap(
            string => {
                string.split(" ")
            }
        )
        println("--------血缘关系----------")
        // new MapPartitionsRDD -> new ParallelCollectionRDD
        println(wordRDD.toDebugString)
        println("--------依赖关系----------")
        //窄依赖OneToOneDependency(1:1)，每一个父RDD的Partition最多被子RDD的一个Partition使用
        println(wordRDD.dependencies)
        val mapRDD = wordRDD.map(
            word => (word, 1)
        )
        println("-------血缘关系-----------")
        // new MapPartitionsRDD -> new MapPartitionsRDD
        println(mapRDD.toDebugString)
        println("--------依赖关系----------")
        // 窄依赖OneToOneDependency(1:1)
        println(mapRDD.dependencies)
        // 如果Spark的计算过程中某一个节点计算失败，那么框架会尝试重新计算
        // Spark既然想要重新计算，那么就需要知道数据的来源，并且还要知道数据经历了哪些计算
        // RDD不保存计算的数据，但是会保存元数据信息
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey( _ + _ )
        println("--------血缘关系----------")
        //  new ShuffledRDD -> new MapPartitionsRDD
        println(reduceRDD.toDebugString)
        println("---------依赖关系---------")
        // 宽依赖ShuffleDependency (N:N)，同一个父RDD的Partition被多个子RDD的Partition依赖
        println(reduceRDD.dependencies)
        println("---------result---------")
        println(reduceRDD.collect().mkString(","))

        sc.stop()

    }
}
