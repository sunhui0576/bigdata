package com.cvcxy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object SparkSQL_RDD_DF_DS {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val jsonDF = spark.read.json("input/user.json")
        // TODO SQL
        // 将df转换为临时视图
        jsonDF.createOrReplaceTempView("user")
        spark.sql("select * from user").show

        // TODO DSL
        jsonDF.select("name", "age").show
        // 查询列名采用单引号，需要导入隐式转换。
        // 这里的spark其实是环境对象的名称
        // 要求这个对象必须使用val声明
        import spark.implicits._
        jsonDF.select($"name", $"age").show
        jsonDF.select('name, 'age).show

        val rdd = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 20),
            (3, "wangwu", 40)
        ))
        // TODO RDD <=> DataFrame
        val df= rdd.toDF("id", "name", "age")
        val dfToRDD= df.rdd

        // TODO RDD <=> DataSet
        val userRDD = rdd.map{ case (id, name, age) => {User(id, name, age)}}
        val userDS = userRDD.toDS()
        val dsToRdd = userDS.rdd

        // TODO DataFrame <=> DataSet
        val dfToDS= df.as[User]
        val dsToDF = dfToDS.toDF()

        // TODO 释放对象
        spark.stop()
    }
    case class User(id:Int, name:String, age:Int)
}
