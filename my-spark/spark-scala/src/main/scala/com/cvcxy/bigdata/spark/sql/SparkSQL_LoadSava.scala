package com.cvcxy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}


object SparkSQL_LoadSava {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // TODO 通用的读取
        // SparkSQL读取默认数据格式为Parquet列式存储
        val parquetToDF: DataFrame = spark.read.load("input/users.parquet")
        // TODO 读取JSON格式,Spark要求JSON文件中每行数据都是一个json
        val jsonToDF: DataFrame = spark.read.format("json").load("input/user.json")
        // TODO 通用的保存
        spark.sql("select * from json.`input/user.json`").show
        // TODO sparksql默认通用保存的文件格式为parquet
        jsonToDF.write.format("json").save("output")
        // 如果非得想要在路径已经存在的情况下，保存数据，那么可以使用保存模式
        jsonToDF.write.mode("append").format("json").save("output")
        spark.stop
    }
}
