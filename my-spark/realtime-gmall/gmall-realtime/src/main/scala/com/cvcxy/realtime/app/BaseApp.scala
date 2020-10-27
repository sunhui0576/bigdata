package com.cvcxy.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait BaseApp {
  /**
   * 提取spark运行环境的创建
   *
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("baseApp")
    val ssc = new StreamingContext(conf,Seconds(3))
    run(ssc)
  }
  def run(ssc:StreamingContext):Unit
}
