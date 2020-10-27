package com.cvcxy.bigdata.scala.distributedComputingFramework

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

object Driver {

    def main(args: Array[String]): Unit = {

        val start = System.currentTimeMillis()
        // TODO 数据准备
        val executorCount = 5
        val myHost = "localhost"
        val myPort = 1234

        // TODO 申请资源
        val resourceCenterRef = new Socket("localhost", 9999)
        //println("资源中心已经连接，数据准备中")
        val resourceCenterRefOut = new ObjectOutputStream(resourceCenterRef.getOutputStream)
        resourceCenterRefOut.writeObject(
            Message(s"executorCount=$executorCount&driverHost=$myHost&driverPort=$myPort"))
        resourceCenterRefOut.flush()
        // 释放和资源中心的连接
        resourceCenterRef.close

        // TODO 接收Executor端的数据
        // 线程安全问题
        //var sum = 0;
        val results: Array[Int] = Array.fill(executorCount)(-1)


        val receiver = new ServerSocket(myPort)

        new Thread(
            new Runnable {
                override def run(): Unit = {
                    // TODO 统计结果的线程
                    var flg = true
                    while (flg) {
                        if ( results.contains(-1) ) {
                            Thread.sleep(100)
                        } else {
                            // TODO 所有的线程都已经计算完毕
                            val end = System.currentTimeMillis()
                            println("计算完毕，结果为 " + results.sum + ",耗时:" + (end-start) + "ms")
                            flg = false
                            System.exit(0)
                        }
                    }
                }
            }
        ).start()

        while ( true ) {
            val executorRef: Socket = receiver.accept()
            //println("执行器已经连接上了。。。")
            new Thread(
                new Runnable {
                    override def run(): Unit = {

                        // TODO  向Executor发送计算任务
                        val executorRefOut =
                            new ObjectOutputStream(executorRef.getOutputStream)
                        val task = new Task()
                        task.logic = _*2
                        executorRefOut.writeObject(task)
                        executorRefOut.flush()
                        executorRef.shutdownOutput()

                        // TODO 获取Executor端计算结果
                        val executorRefIn =
                            new ObjectInputStream(executorRef.getInputStream)
                        val m: Message = executorRefIn.readObject().asInstanceOf[Message]
                        val datas: Array[String] = m.content.split("=|&")
                        // executorId=${id}&result=$i
                        // executorId
                        // id
                        // result
                        // i
                        results(datas(1).toInt-1) = datas(3).toInt
                        //println("获取计算结果 = " + result)
                    }
                }
            ).start()
        }

    }
}
