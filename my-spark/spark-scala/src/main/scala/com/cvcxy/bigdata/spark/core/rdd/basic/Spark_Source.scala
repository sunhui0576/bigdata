package com.cvcxy.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Source {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // TODO  1.  Spark - 从内存中创建RDD
        // 1.1 parallelize : 并行
        val list = List(1,2,3,4)
        sc.parallelize(list)

        // 1.2 makeRDD的底层代码其实就是调用了parallelize方法
        sc.makeRDD(list)

        // 1.3 RDD中的分区的数量就是并行度，设定并行度，其实就在设定分区数量
        // 内存中的集合数据如果不能平均分，会将多余的数据放置在最后一个分区
        sc.makeRDD(List(1,2,3,4),3)

        // TODO 2. Spark - 从磁盘（File）中创建RDD - 单文件
        // 第二个参数表示最小分区数量，默认值为： math.min(defaultParallelism, 2)
      //  Spark读取文件采用的是Hadoop的读取规则
      //    文件切片规则 :  以字节方式来切片
      //    数据读取规则 ： 以行为单位来读取
      //   所谓的最小分区数，取决于总的字节数是否能整除分区数并且剩余的字节达到一个比率
      //   实际产生的分区数量可能大于最小分区数
      //   分区数据是以行为单位读取的是，而不是字节
      // TODO 案例
      // 数据：w.txt中数据存储分析
      //    数据是以行的方式读取，但是会考虑偏移量（数据的offset）的设置，@@表示换行符，占两个字节
      //    1@@ => 012
      //    2@@ => 345
      //    3@@ => 678
      //    4   => 9
      //w.txt分区数 = 最小分区数 + (字节数 % 最小分区数) / ( 字节数/最小分区数 ) > 0.1 ? 1 : 0
      //           = 4 + (10%4) / (10/4) > 0.1 ? 1 : 0
      //           = 4 + 2 / 2 > 0.1 ? 1 : 0 = 5
      //    w.txt => (0, 2) => 1
      //    w.txt => (2, 4) => 2
      //    w.txt => (4, 6) => 3
      //    w.txt => (6, 8) =>
      //    w.txt => (8,10) => 4

        sc.textFile("input/word*.txt")
        sc.textFile("input/w.txt",4)

        // TODO 3. Spark - 从磁盘（File）中创建RDD -多文件
        // hadoop分区是以文件为单位进行划分的。读取数据不能跨越文件
        // 1@@ => 012
        // 234 => 345

        // 12 / 3 = 4
        // 1.txt => (0, 4) =>   1
        //                      234
        // 1.txt => (4, 6) =>
        // 2.txt => (0, 4) =>   1
        //                      234
        // 2.txt => (4, 6) =>
        val fileRDD1: RDD[String] = sc.textFile("input/files", 3)
        val unit = fileRDD1.saveAsTextFile("output")
        sc.stop()
    }
}
