package com.cvcxy.summer.framework.util

import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object EnvUtil {
    //TODO ThreadLocal:java api提供的工具类，可以访问线程的可以共享数据的内存空间
    private val scLocal = new ThreadLocal[SparkContext]
    private val sscLocal = new ThreadLocal[StreamingContext]

    def getSSCEnv( time : Duration = Seconds(5) ) = {
        // 从当前线程的共享内存空间中获取环境对象
        var ssc: StreamingContext = sscLocal.get()
        // 如果没有从当前线程的共享内存获取到环境对象
        if ( ssc == null ) {
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkApplication")
            // 创建新的环境对象
            ssc = new StreamingContext(sparkConf, time)
            // 保存新创建的环境对象到共享内存中
            sscLocal.set(ssc)
        }
        ssc
    }
    def getSCEnv()  = {
        // 从当前线程的共享内存空间中获取环境对象
        var sc = scLocal.get()
        if ( sc == null ) {
            // 如果没有从当前线程的共享内存获取到环境对象
            val sparkConf = new SparkConf().setMaster("local").setAppName("sparkApplication")
            // 创建新的环境对象
            sc = new SparkContext(sparkConf)
            // 保存新创建的环境对象到共享内存中
            scLocal.set(sc)
        }
        sc
    }

    def clear(): Unit = {
        getSCEnv.stop()
        // 将共享内存中的数据清除掉
        scLocal.remove()
    }

}
