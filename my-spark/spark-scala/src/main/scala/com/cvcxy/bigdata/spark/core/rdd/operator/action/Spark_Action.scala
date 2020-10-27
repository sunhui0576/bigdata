package com.cvcxy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Action {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark Action 行动算子，不会产生新的RDD，返回结果集
        //  触发Job，runJob，然后提交这个Job对象，submitJob
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val rdd1: RDD[String] = sc.makeRDD(List("a","a","a","hello", "hello"))
        val kv: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 2)),1)
        // TODO reduce 和scala的reduce功能相同，两两计算
        rdd.reduce(_+_)
        // TODO collect 将executor端计算结果都拉到driver端，可能oom
        rdd.collect()
        // TODO count 计数
        rdd.count()
        // TODO takeOrdered 排序后取前3个
        rdd.takeOrdered(3)
        // TODO sum 求和
        rdd.sum()
        // TODO first 获取第一个数
        // TODO aggregate 初始值分区内和分区间计算都会参与第一个数计算
        rdd.aggregate(10)(_+_,_+_) //result 40
        // TODO fold
        rdd.fold(10)(_+_)
        // TODO countByKey
        kv.countByKey()
        // TODO countByValue
        rdd1.countByValue()
        // TODO saveAs
        rdd1.saveAsTextFile("output")
        rdd1.saveAsObjectFile("output1")
        kv.saveAsSequenceFile("output2")
        // TODO foreach scala方法,当前节点执行
        rdd.collect().foreach(println)
        println("*******************************")
        // TODO foreach spark算子，executor端执行
        rdd.foreach(println)
        sc.stop()
    }
}
