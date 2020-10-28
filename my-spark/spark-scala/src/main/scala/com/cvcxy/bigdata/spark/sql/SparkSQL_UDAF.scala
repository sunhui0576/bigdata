package com.cvcxy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField, StructType}


object SparkSQL_UDAF {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30L),
            (2, "lisi", 20L),
            (3, "wangwu", 40L)
        )).toDF("id", "name", "age")
        // TODO SQL方式：UserDefinedAggregateFunction
        df.createOrReplaceTempView("user")
        val udaf = new MyAvgAgeUDAF
        // 注册到SparkSQL中
        spark.udf.register("avgAge", udaf)
        spark.sql("select avgAge(age) from user").show
        // TODO DSL方式：通过继承Aggregator[User, AvgBuffer, Long]，传入类型，使得代码更易读，数据类型明确，不容易出错
        val ds = df.as[User]
        val udafClass = new MyAvgAgeUDAFClass
        // 这种方法不能使用注册到spark中，写sql的方式，可以采用DSL语法方法进行访问
        // 将自定义聚合函数MyAvgAgeUDAFClass转为列
        // MyAvgAgeUDAFClass中是以和数据的结构相同的样例类定义处理方法的
        ds.select(udafClass.toColumn).show

        spark.stop()
    }
    class MyAvgAgeUDAF extends UserDefinedAggregateFunction{
        override def inputSchema: StructType = {
            StructType(Array(StructField("age", LongType)))
        }
        // 定义缓冲区所需变量
        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("totalage", LongType),
                StructField("count", LongType)
            ))
        }
        // 聚合函数返回的结果类型
        override def dataType: DataType = LongType
        //函数稳定性：多次重复数据结果是否一致
        override def deterministic: Boolean = true
        // 给bufferSchema中定义的变量给个初始化值
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 0L
        }
        // task内，年龄相加，人数加一
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1
        }
        // 多个task之间，年龄相加，人数相加
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
            buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }
        // 合并后的计算规则：平均年龄 = 年龄之和/总人数
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0) / buffer.getLong(1)
        }
    }

    case class User( id:Int, name:String, age:Long )
    case class AvgBuffer( var totalage:Long, var count:Long )

    /**
     * 1.通过继承Aggregator[User, AvgBuffer, Long]，传入类型，使得代码更易读，数据类型明确，不容易出错
     */
    class MyAvgAgeUDAFClass extends Aggregator[User, AvgBuffer, Long]{
        // 对传入的缓冲区变量初始化
        override def zero: AvgBuffer = {
            AvgBuffer(0L,0L)
        }
        // task内，年龄相加，人数加一
        override def reduce(buffer: AvgBuffer, user: User): AvgBuffer = {
            buffer.totalage = buffer.totalage + user.age
            buffer.count = buffer.count + 1
            buffer
        }
        // 多个task之间，年龄相加，人数相加
        override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
            buffer1.totalage = buffer1.totalage + buffer2.totalage
            buffer1.count = buffer1.count + buffer2.count
            buffer1
        }
        // 合并后的计算规则：平均年龄 = 年龄之和/总人数
        override def finish(reduction: AvgBuffer): Long = {
            reduction.totalage / reduction.count
        }
        // 自定义类固定 Encoders.product
        override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
        // scala自带类，Long对应Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
