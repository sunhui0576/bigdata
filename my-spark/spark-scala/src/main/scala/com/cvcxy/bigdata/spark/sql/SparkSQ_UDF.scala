package com.cvcxy.bigdata.spark.sql


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSQ_UDF {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        val rdd = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 20),
            (3, "wangwu", 40)
        ))
        val df = rdd.toDF("id", "name", "age")
        df.createOrReplaceTempView("user")
//        val ds: Dataset[Row] = df.map(row => {
//            val id = row(0)
//            val name = row(1)
//            val age = row(2)
//            Row(id, "name : " + name, age)
//        })
        // 尽量使用dataset 因为row类型因为编码问题，如上操作会报错
        val userRDD = rdd.map{
            case (id, name, age) => {
                User(id, name, age)
            }
        }
        val userDS= userRDD.toDS()
        val newDS = userDS.map(user=>{
            User( user.id, "name:" + user.name, user.age )
        })
        newDS.show

        // TODO 使用 自定义函数在SQL中完成数据的转换操作
        spark.udf.register("addName",(x:String)=> "Name:"+x)
        spark.udf.register("changeAge",(x:Int)=> 18)

        spark.sql("select addName(name), changeAge(age) from user").show
        spark.stop()
    }
    case class User(id:Int, name:String, age:Int)
}
