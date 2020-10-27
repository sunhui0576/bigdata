package com.cvcxy.realtime.util

import java.util.Properties

/**
 * 获取fileName中name配置项
 */
object ConfigUtil {
  def getProperty(fileName:String,name:String)={
    val is = ConfigUtil.getClass.getClassLoader.getResourceAsStream(fileName)
    val ps = new Properties()
    //加载fileName
    ps.load(is)
    //获取name配置
    ps.getProperty(name)
  }

  def main(args: Array[String]): Unit = {
    println(ConfigUtil.getProperty("config.properties", "kafka.servers"))
  }
}
