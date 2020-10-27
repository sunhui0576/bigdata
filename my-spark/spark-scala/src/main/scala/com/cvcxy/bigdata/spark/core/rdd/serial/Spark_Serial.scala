package com.cvcxy.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Serial {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)
        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
        // TODO Spark 序列化
        /**如果用到driver端的对象，对象必须extends Serializable
         * 样例类自动混入可序列化特征
        // TODO Scala 闭包检测
         * spark的算子操作都是闭包
         * 提交作业前ensureSerializable方法中，尝试序列化
         * 如果不能序列化，直接报错
         * if (checkSerializable) {
         * ensureSerializable(func)
         * }
         * SparkEnv.get.closureSerializer.newInstance().serialize(func)
         */
        // TODO 查找字符串
        val search = new Search("hello")
        //函数传递，类需要序列化
        search.getMatch1(rdd).collect()
        //属性传递，类需要序列化
        search.getMatch2(rdd).collect()
        // TODO kryo序列化比java序列化速度快10倍,序列化后文件小9倍
        //  spark2.0内部使用kryo，shuffle数据时速度更快
        sc.stop()
    }
    class Search(query:String) extends Serializable {
        def isMatch(s: String): Boolean = {
            s.contains(query)
        }
        def getMatch1 (rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            rdd.filter(x => x.contains(query))
        }
    }
}
