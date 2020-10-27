package com.cvcxy.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_Accumulator_And_Broadcast {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        // TODO 累加器 ： 分布式共享executor只写变量,driver端可读
        // 2. 累加器在executor端只增加数据，不做数据的计算，因为累加器的值不可读
        // 3. 计算完毕后，executor会将累加器的计算结果返回到driver端。
        // 4. driver端获取到多个累加器的结果，然后两两合并。最后得到累加器的执行结果。
        val rdd = sc.makeRDD(List(1,2,3,4))
        val rdd1 = sc.makeRDD(List("hello scala", "hello spark", "spark", "hello", "hello", "spark","scala"),2)
      val rdd2 = sc.makeRDD(List(("a",1),("b",2), ("c",3)))
      val list = List(("a",4),("b",5), ("c",6))
      // 声明累加器变量
        val sum: LongAccumulator = sc.longAccumulator("sum")

        rdd.foreach(num => {
                sum.add(num)
            })
        println("结果为 = " + sum.value)
        // TODO WordCount

        // 1. 创建累加器
        val acc = new MyWordCountAccumulator
        // 2.  注册累加器
        sc.register(acc)
        //  3. 使用累加器
        rdd1.flatMap(_.split(" ")).foreach{ word => {acc.add(word)}}
        // 4. 获取累加器的值
        println(acc.value)
      // TODO 广播变量：分布式共享executor端只读变量
      // 两个数据集join，将小表加载到内存
      // 使用普通变量，会使得每个task都存储一份数据，数据冗余
      // 声明广播变量，executor存一份，每个task都可以访问到
      val bcList: Broadcast[List[(String, Int)]] = sc.broadcast(list)
      val rdd3 = rdd2.map{
        case ( word, count1 ) => {
          var count2 = 0
          for ( kv <- bcList.value ) {
            val w = kv._1
            val v = kv._2
            if ( w == word ) {
              count2 = v
            }
          }
          (word, (count1, count2))
        }
      }
      println(rdd3.collect().mkString(","))

        sc.stop()
    }
    // TODO 自定义累加器
    // 1. 继承 AccumulatorV2，定义泛型 [IN, OUT]
    class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {
        // 存储WordCount的集合
        var wordCountMap = mutable.Map[String, Int]()
        // 累加器是否初始化
        override def isZero: Boolean = {
            wordCountMap.isEmpty
        }
        // 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
            new MyWordCountAccumulator
        }
        // 重置累加器
        override def reset(): Unit = {
            wordCountMap.clear()
        }
        // 分区内累加器数据处理规则
        override def add(word: String): Unit = {
            wordCountMap.update(word, wordCountMap.getOrElse(word, 0) + 1)
        }
        // 分区间累加器合并规则，//task完成后，driver调用
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
            val map1 = wordCountMap
            val map2 = other.value
          // 下一个累加器去关联当前累加器，并返回合并后的累加器
            wordCountMap = map1.foldLeft(map2)(
                (map, kv) => {
                  // 更新统计值 map(kv._1)=2 是mutable，相当于map.update(kv._1,2)
                    map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
                    map
                }
            )
        }
        // 返回累加器的值（Out）
        override def value: mutable.Map[String, Int] = {
            wordCountMap
        }
    }
}
