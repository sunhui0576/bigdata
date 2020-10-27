package com.cvcxy.bigdata.spark.core.req.service

import com.cvcxy.bigdata.spark.core.req.bean
import com.cvcxy.bigdata.spark.core.req.bean.HotCategory
import com.cvcxy.bigdata.spark.core.req.dao.{HotCategoryAnalysisTop10Dao, HotCategorySessionAnalysisTop10Dao}
import com.cvcxy.bigdata.spark.core.req.helper.HotCategoryAccumulator
import com.cvcxy.summer.framework.util.EnvUtil
import com.cvcxy.summer.framework.core.TService
import com.cvcxy.summer.framework.util.EnvUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class HotCategorySessionAnalysisTop10Service extends TService{

    private val hotCategorySessionAnalysisTop10Dao = new HotCategorySessionAnalysisTop10Dao

    /**
      * 数据分析
      * @return
      */
    override def analysis( data : Any )  = {

        val top10: List[HotCategory] = data.asInstanceOf[List[HotCategory]]
        val top10Ids: List[String] = top10.map(_.categoryId)
        // 使用广播变量实现数据的传播
        val bcList: Broadcast[List[String]] = EnvUtil.getSCEnv().broadcast(top10Ids)
        //  获取用户行为的数据
        val actionRDD: RDD[bean.UserVisitAction] = hotCategorySessionAnalysisTop10Dao.getUserVisitAction("input/user_visit_action.txt")
        // 过滤出属于前十品类的数据
        actionRDD.filter(
            action => {
                if (action.click_category_id != -1) {
                    bcList.value.contains(action.click_category_id.toString)
                } else {
                    false
                }
            }
        //结构转换为：品类_session，对联合key分组聚合
        ).map(
            action => {
                (action.click_category_id + "_" + action.session_id, 1)
            }
        ).reduceByKey(_+_)
        // 拆分联合key (品类_session, sum) 转为 (品类, (session, sum))，分组
        .map {
            case (key, count) => {
                val ks: Array[String] = key.split("_")
                (ks(0), (ks(1), count))
            }
        }.groupByKey()
        // 排序取前10名
        .mapValues(iter => {
                iter.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(10)
            }
        ).collect()
    }

}
