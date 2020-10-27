package com.cvcxy.bigdata.spark.core.rdd.operator.transfer

import com.cvcxy.bigdata.spark.core.req.dao.HotCategoryAnalysisTop10Dao
import org.apache.spark.rdd.RDD

object mapAndMapPartition {
  private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao
  def main(args: Array[String]): Unit = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")

    actionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).filter( _._1 != "-1" ).foreach(println)

    actionRDD.mapPartitions(
      (actions: Iterator[String]) =>{
        // TODO List[(String, Int)]() ：list的元素类型是元组(String, Int)，后面加上()调用apply方法
        var result = List[(String, Int)]()
        while (actions.hasNext){
          val datas = actions.next().split("_")
          if (datas(6) != "-1") {
            result .::= (datas(6),1)
          }
        }
        result.reverse.iterator
      }
    ).foreach(println)
  }
}
