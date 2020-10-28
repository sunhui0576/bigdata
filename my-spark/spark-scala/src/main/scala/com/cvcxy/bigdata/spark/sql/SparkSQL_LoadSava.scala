package com.cvcxy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SaveMode, SparkSession}


object SparkSQL_LoadSava {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // TODO 通用的读取 默认Parquet列式存储
        val parquetToDF: DataFrame = spark.read.load("input/users.parquet")
        // 直接文件上查询，格式："sql 文件格式.`文件路径`"
        spark.sql("select * from json.`input/user.json`").show
        // TODO 读取JSON格式,Spark要求JSON文件中每行数据都是一个json
        val jsonToDF: DataFrame = spark.read.format("json").load("input/user.json")
        // TODO 通用的保存 默认Parquet列式存储
        jsonToDF.write.save("output")
        // TODO 其他格式需要指定format
        jsonToDF.write.format("json").save("output")
        // 也可以再已存在的目录中保存文件，并选择保存模式
        jsonToDF.write.mode("append").format("json").save("output")
        val frame: DataFrame = spark.read.format("csv")
          .option("sep", ";")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("input/user.csv")
        frame.show
        spark.read.format("jdbc")
          .option("url", "jdbc:mysql://linux1:3306/spark-sql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "123123")
          .option("dbtable", "user")
          .load().show
        frame.write.format("jdbc")
          .option("url", "jdbc:mysql://linux1:3306/spark-sql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "123123")
          .option("dbtable", "user1")
          .mode(SaveMode.Append)
          .save()

        // TODO 默认情况下SparkSQL支持本地Hive操作的，执行前需要启用Hive的支持
        // 调用enableHiveSupport方法。
        val hviespark = SparkSession.builder()
          .enableHiveSupport()
          .config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称

        // 可以使用基本的sql访问hive中的内容
        //spark.sql("create table aa(id int)")
        //spark.sql("show tables").show()
        hviespark.sql("load data local inpath 'input/id.txt' into table aa")
        hviespark.sql("select * from aa").show
        // TODO 访问外置的Hive
        // 导入隐式转换，这里的spark其实是环境对象的名称
        hviespark.sql("show databases").show
        //spark.sql("use atguigu200213")

        hviespark.sql(
            """
              |CREATE TABLE `user_visit_action`(
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        hviespark.sql(
            """
              |load data local inpath 'input/user_visit_action.txt' into table user_visit_action
            """.stripMargin)

        hviespark.sql(
            """
              |CREATE TABLE `product_info`(
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        hviespark.sql(
            """
              |load data local inpath 'input/product_info.txt' into table product_info
            """.stripMargin)

        hviespark.sql(
            """
              |CREATE TABLE `city_info`(
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        hviespark.sql(
            """
              |load data local inpath 'input/city_info.txt' into table city_info
            """.stripMargin)

        hviespark.sql(
            """
              |select * from city_info
            """.stripMargin).show(10)
        hviespark.stop
        spark.stop
    }
}
