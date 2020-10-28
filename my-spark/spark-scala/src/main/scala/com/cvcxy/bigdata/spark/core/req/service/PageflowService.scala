package com.cvcxy.bigdata.spark.core.req.service

import com.cvcxy.bigdata.spark.core.req.bean
import com.cvcxy.bigdata.spark.core.req.dao.PageflowDao
import com.cvcxy.summer.framework.core.TService
import org.apache.spark.rdd.RDD

class PageflowService extends TService {

    private val pageflowDao = new PageflowDao
    /**
      * 页面单挑转化率
      * @return
      */
    override def analysis() = {
        //部分页面id
        val flowIds = List(1,2,3,4,5,6,7)
      //这部分id手动zip
        val okFlowIds = flowIds.zip(flowIds.tail).map( t => (t._1 + "-" + t._2) )
      //读取数据
        val actionRDD: RDD[bean.UserVisitAction] = pageflowDao
          .getUserVisitAction("input/user_visit_action.txt")
          .cache()
        // TODO 计算分母
        //因为我们的每条数据就是一个页面访问记录，只要页面id,分组求和，就得到每个页面的点击次数
        val pageCountArray = actionRDD
          //如果只需要统计部分页面，在此处过滤
          //需要注意的是：使用contains方法，要注意比较的类型是否一致
          //init表示去除最后一个数据
//          .filter(action => {flowIds.init.contains(action.page_id.toInt)})
          .map(action => {(action.page_id, 1)})
          .reduceByKey(_ + _)
          .collect()
        // TODO 计算分子
        // 按会话分组
        val pageFlowCountArray = actionRDD.groupBy(_.session_id)
          .mapValues(
              iter => {
                  val pageids = iter
                    .toList
                    // 调用list的排序方法sortwith，按点击时间排序
                    .sortWith((left, right) => {left.action_time < right.action_time})
                    .map(_.page_id)
                  //zip关联，pageids(1)关联pageids(2)，pageids(2)关联pageids(3)
                  pageids.zip(pageids.tail)
                    .map { case (pageid1, pageid2) => {(pageid1 + "-" + pageid2, 1)}}
                  //如果只需要统计部分页面，在此处过滤
                //在数据组合好再过滤
//                  .filter{ case ( ids, _) => {okFlowIds.contains(ids)}}
              }
          )
          //只保留：(首页-详情，count),因为跳转率统计是全部session的跳转数据放在一起统计的
          .map(_._2)
          // RDD[List([(String,Int)])] =>RDD[(String,Int)],方便分组求和
          .flatMap(list => list)
          .reduceByKey(_ + _)
        // 页面跳转统计遍历，计算每种跳转的跳转率
        // 分母：页面点击次数统计
        // 分子：连续的两个点击形成单挑，次数统计
        // 例如：首页-详情/首页 ：首页跳转到详情的次数/首页被电机的次数
        pageFlowCountArray.foreach{
                case ( pageflow, sum ) => {
                    println("页面跳转【"+pageflow+"】的转换率为: "
                      + (sum.toDouble / pageCountArray.toMap.getOrElse(pageflow.split("-")(0).toLong,1)))
                }
            }
    }
}
