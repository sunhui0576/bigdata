package com.cvcxy.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Persist {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        sparkConf.set("spark.local.dir.blockmgr", "D:\\mineworkspace\\idea\\classes\\classes-0213\\input")
        val sc = new SparkContext(sparkConf)


        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO 缓存cache底层其实调用的persist方法
        // 内存不够用，executor可以将内存的数据进行整理，然后可以丢弃数据。
        // cache失效，重头执行，遵循血缘关系，cache不能删除血缘关系。
        // cache操作在行动算子执行后，会在血缘关系中增加和缓存相关的依赖
       val cacheRDD = rdd.map( num=>{(num,1)}).cache()
        println(cacheRDD.toDebugString)
        println(cacheRDD.collect().mkString(","))
        println(cacheRDD.toDebugString)
        // TODO 使用checkpoint方法将数据保存到文件中,需要设定检查点的保存目录
        // 检查点的操作中为了保证数据的准确性，会执行时，会启动新的job
        // 检查点和cache联合使用，。。。，提高性能
        // 检查点操作会切断血缘关系，因为检查点会将数据保存到分布式存储系统中，数据不容易丢失。
        // 等同于产生新的数据源
        cacheRDD.checkpoint()
        sc.stop()

    }
}
