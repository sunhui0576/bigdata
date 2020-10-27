package com.cvcxy.bigdata.scala.distributedComputingFramework

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

object ResourceCenter {

    def main(args: Array[String]): Unit = {

        // TODO 1. 创建服务，接收资源请求
        val receiver = new ServerSocket(9999)
        println("资源中心已经启动。。。")
        while ( true ) {
            val driverRef: Socket = receiver.accept()
            //来一个请求，启动一个线程处理
            // java8 新特性 Lambda表达式实现接口
            new Thread(
                () => {
                    // TODO 接收Driver传递的数据
                    val driverRefIn = new ObjectInputStream(driverRef.getInputStream)
                    val message: Message = driverRefIn.readObject().asInstanceOf[Message]
                    // executorCount=5&driverHost=localhost&driverPort=1234
                    // 分解字符串，应该同时使用 =，&
                    // 使用正则表达式分解字符串
                    val datas: Array[String] = message.content.split("=|&")
                    val executorCount = datas(1).toInt
                    val driverHost = datas(3)
                    val driverPort = datas(5).toInt

                    for (i <- 1 to executorCount) {
                        val executor = new Executor(i, driverHost, driverPort)
                        executor.startup()
                    }
                }
            ).start()
        }
    }
}
