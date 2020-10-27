package com.cvcxy.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark_RDD_Operator {

    def main(args: Array[String]): Unit = {

        // 0环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)
        // 1source
        val rddPar: RDD[Int] = sc.makeRDD(List(1,7,3,4,8),2)
        val rddPar0: RDD[Int] = sc.makeRDD(List(8,5,2,4,7),2)
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))
        val rddPar1 = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)
        val rddPar2 = sc.makeRDD(List("Hello world", "Hello hive", "Hello hbase", "Hello Hadoop"), 2)
        val kv: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 2)),1)
        val kv1: RDD[(String, Int)] = sc.makeRDD(List(("b", 1), ("c", 3)),1)

      val fileRDD = sc.textFile("input/apache.log")
      val fileRDD1 = sc.textFile("input/agent.log")
        val dataRDD = sc.makeRDD(List(
            List(1,2),List(3,4)
        ))
        val dataRDD1 = sc.makeRDD(
            List(List(1,2),3,List(4,5))
        )
        // 2transform
        // TODO 2.1 map
        // 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据
        // 分区间数据执行没有顺序，而且无需等待
        rddPar.map( _ * 2 )
        fileRDD.map(
            line => {
                val datas: Array[String] = line.split(" ")
                datas(6)
            }
        )
        // TODO 2.1 mapPartitions
        // 一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作,过滤，max,sum
        // map算子每一次处理一条数据，而mapPartitions算子每一次将一个分区的数据当成一个整体进行数据处理。
        // 如果一个分区的数据没有完全处理完，那么所有的数据都不会释放，即使前面已经处理完的数据也不会释放。
        // 容易出现内存溢出，所以当内存空间足够大时，为了提高效率，推荐使用mapPartitions算子
        rddPar.mapPartitions(
            iter => {
                iter.filter(_ % 2 == 0)
            }
        )
        // 获取每个数据分区的最大值
        rddPar.mapPartitions(
            iter => {
                List(iter.max).iterator
            }
        )
        // 获取每个分区最大值以及分区号
        rddPar.mapPartitionsWithIndex(
            (index, iter) => {
                List((index, iter.max)).iterator
            }
        )
        // 获取第二个数据分区的数据
        rddPar.mapPartitionsWithIndex(
            (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator
                }
            }
        )
        // TODO 2.1 flatMap
        dataRDD.flatMap(list=>list)
        dataRDD1.flatMap(
            data => {data match {
                    case list: List[_] =>list
                    case d => List(d)
                }
            }
        )
        // TODO 2.1 glom => 将每个分区的数据转换为数组
        // int ->Array[Int],由一次处理一条数据，变为一次处理一个分区的数据的数组
        // 和mappartition有什么区别，功能类似
        val glomRDD: RDD[Array[Int]] = rddPar.glom()
//        glomRDD.foreach(
//            array => {
//                println(array.mkString(","))
//            }
//        )
        // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
      rddPar.glom().map(array => array.max).collect().sum
        // TODO 2.1 groupBy 方法可以根据指定的规则进行分组，指定的规则的返回值就是分组的key
        // 返回值为元组
        //      元组中的第一个元素，表示分组的key
        //      元组中的第二个元素，表示相同key的数据形成的可迭代的集合
        // groupBy方法执行完毕后，会将数据进行分组操作，但是分区是不会改变的。
        //      不同的组的数据会打乱在不同的分区中
        // groupBy方法方法会导致数据不均匀，产生shuffle操作。如果想改变分区，可以传递参数。
        rddPar.groupBy(
            (x) => {
                x - 2
            }, 2
        )
        // 根据单词首写字母进行分组
        rddPar1.groupBy(word=>{
            //            word.substring(0,1)
            //            word.charAt(0)
            //隐式转换：String(0) => StringOps
            word(0)
        })
        //wordcount
        rddPar2.flatMap(_.split(" ")).groupBy(word=>word).map{case (word,iter)=>{(word,iter.size)}}
        rddPar2.flatMap(_.split(" ")).groupBy(word=>word).map(kv=>(kv._1,kv._2.size))
        rddPar2.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
        // TODO 2.1 filter
        rddPar.filter(
            num => {
                num % 2 == 0
            }
        )
      // 从服务器日志数据apache.log中获取2015年5月17日的请求路径
      fileRDD.map(_.split(" ")).filter(x => {
        val str = x(3).substring(0,10)
        str == "17/05/2015"
      }).map(data=>data(6))

        // TODO 2.1 sample用于从数据集中抽取数据
        // 抽取数据不放回（伯努利算法）
        // 伯努利算法：又叫0、1分布。例如扔硬币，要么正面，要么反面。
        // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
        // 第一个参数：抽取的数据是否放回，false：不放回
        // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
        // 第三个参数：随机数种子,种子数值不变，则每次随机的数一样
        rdd.sample(false, 0.5)
        // 抽取数据放回（泊松算法）
        // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
        // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
        // 第三个参数：随机数种子,种子数值不变，则每次随机的数一样
        rdd.sample(true, 2)

        // TODO 2.1 distinct 去重
        // distinct可以改变分区的数量
        rdd.distinct(2)

        // TODO 2.1 缩减分区 : coalesce
      //当发现数据分区不合理或者数据过滤后，发现数据不够均匀，那么可以缩减分区
      rdd.coalesce(2)
      //扩大分区，一般不这么用
      rdd.coalesce(6, true)
        // TODO 2.1 扩大分区 ： repartition
      //repartition其实就是coalesce(6, true)
      rdd.repartition(6)
        // TODO 2.1 sortBy
        // sortBy可以通过传递第二个参数改变排序的方式
        // sortBy可以设定第三个参数改变分区。
      rdd.sortBy(num=>num, false,3)
        // TODO 2.1 双value类型
      // 并集, 数据合并，分区也会合并
      rddPar.union(rddPar0)
      // 交集 ： 保留最大分区数 ，数据被打乱重组, shuffle
     rddPar.intersection(rddPar0)
      // 差集 : 数据被打乱重组, shuffle
      // 当调用rdd的subtract方法时，以当前rdd的分区为主，所以分区数量等于当前rdd的分区数量
      rddPar.subtract(rddPar0)
      // 拉链 : 分区数不变，两个RDD分区和数据量必须完全相同
      // 2个RDD的分区一致,但是数据量相同的场合:
      //   Exception: Can only zip RDDs with same number of elements in each partition
      // 2个RDD的分区不一致，数据量也不相同，但是每个分区数据量一致：
      //   Exception：Can't zip RDDs with unequal numbers of partitions: List(3, 2)
      rddPar.zip(rddPar0)
        // TODO 2.1 K V类型的数据操作
      /**RDD的伴生对象中提供了隐式函数可以将RDD[K,V]转换为PairRDDFunction类，扩展RDD功能
       * implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
       * (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
       * new PairRDDFunctions(rdd)
       * }
       */
      // TODO partitionBy : 根据指定的规则对数据进行分区
      // partitionBy参数为分区器对象 ： HashPartitioner & RangePartitioner
      // HashPartitioner是spark默认的分区器，将当前数据的key进行取余操作
      kv.partitionBy( new HashPartitioner(2) )
      // RangePartitioner要求数据能够排序，使用较少，sortBy使用到了RangePartitioner
      // 自定义分区器 - 自己决定数据放置在哪个分区做处理
      kv.partitionBy(new MyPartitioner(3))
          .mapPartitionsWithIndex(
            (index,datas)=>{
              datas.map(
                data =>(index,data)
              )
            }
          )

      /**多次分区，如果和当前分区器一样不重分区，否则重分区(Shuffle)
       * if (self.partitioner == Some(partitioner)) {
       * self
       * } else {
       * new ShuffledRDD[K, V, V](self, partitioner)
       * }
       * scala中self.partitioner == Some(partitioner)，双等号等于equals
       * HashPartitioner重写了equals,比较两个分区器对象的类型和分区数是否相同
       * override def equals(other: Any): Boolean = other match {
       * case h: HashPartitioner =>
       *       h.numPartitions == numPartitions
       * case _ =>
       * false
       * }
       */
      // TODO reduceByKey : 分区内和分区间计算规则一样,初始值为0
      //  上游task预聚合(分区内和分区间计算规则一样)，减少了落盘数据量，shuffle过程更快,shuffle默认是有缓存的
      kv.reduceByKey(_+_,2)
      // TODO groupByKey : 根据数据的key进行分组
      kv.groupByKey().map{
        case ( word, iter ) => {
          (word, iter.sum)
        }
      }
      // TODO aggregateByKey ： 分区内和分区间计算规则不一样，可以给初始值
      //  (seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]
      // 分区内相同key取最大值，分区间相同的key求和
      kv.aggregateByKey(10)(
        (x, y) => math.max(x, y),
        (x, y) => x + y
      )
      // TODO foldByKey 分区内和分区间计算规则一样,可以给初始值
      kv.foldByKey(0)(_+_)
      // TODO combineByKey 分区内和分区间计算规则不一样
      // 求平均值
      kv.combineByKey(
        // 将计算的第一个值转换结构
        v => (v, 1),
        // 分区内：第一个值和后面的值相加，计数累加
        //  sum  count
        (t: (Int, Int), v) => {
          //值相加    计数加1
          (t._1 + v, t._2 + 1)
        },
        // 分区间
        (t1: (Int, Int), t2: (Int, Int)) => {
          (t1._1 + t2._1, t1._2 + t2._2)
        }
      ).map{
        case ( key, ( total, cnt ) ) => {
          (key, total / cnt)
        }
      }

      /**
       * reduceByKey、aggregateByKey、foldByKey、combineByKey
       * 底层逻辑都是combineByKeyWithClassTag，只是参数不同
       * mergeValue: (C, V) => C, 对第一个value处理
       * mergeCombiners: (C, C) => C, 分区内计算规则
       * partitioner: Partitioner, 分区间计算规则
        */

      // TODO sortByKey K必须实现Ordered接口
      /**
       * class User extends Ordered[User]{
       * override def compare(that: User): Int = {
       * 1
       * }
       * }
       */
        kv.sortByKey(true)
      // TODO join 和数据库join区别，join字段可以重复，多次连接
      kv.join(kv1)
      // TODO leftOuterJoin
      kv.leftOuterJoin(kv1)
      // TODO rightOuterJoin
      kv.rightOuterJoin(kv1)
      // TODO cogroup 返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
      //rdd内部相同key分组，然后rdd之间相同key连接
      kv.cogroup(kv1)
      // TODO 统计出每一个省份每个广告被点击数量排行的Top3
      // 结构转换 (（省份 - 广告）,1)
      fileRDD1.map(
        line => {
          val datas = line.split(" ")
          (datas(1) + "-" + datas(4),1)
        }
      )
      // 分组聚合 (（省份 - 广告）,sum)
     .reduceByKey(_+_)
      // 结构转换( 省份，（广告，sum） )
      .map{
        case ( key, sum ) => {
          val keys = key.split("-")
          ( keys(0), ( keys(1), sum ) )
        }
      }
      // 按省份分组( 省份，Iterator[（广告1，sum1）,（广告2，sum2）] )
      .groupByKey()
      // 排序（降序），取前3 Top3
      // mapValues //key不变，对value操作
      .mapValues(iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      })
      sc.stop()
    }
  // TODO 自定义分区器
  // 1. 和Partitioner类发生关联，继承Partitioner
  // 2. 重写方法
  class MyPartitioner(num:Int) extends Partitioner {
    // 获取分区的数量
    override def numPartitions: Int = {num}
    override def getPartition(key: Any): Int = {
      // 根据数据的key来决定数据在哪个分区中进行处理
      // 方法的返回值表示分区编号（索引）
      key match {
        case "nba" => 1
        case _ => 0
      }
    }
  }
}
