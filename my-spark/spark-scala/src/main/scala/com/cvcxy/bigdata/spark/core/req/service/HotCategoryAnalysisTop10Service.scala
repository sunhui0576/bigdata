package com.cvcxy.bigdata.spark.core.req.service

import com.cvcxy.bigdata.spark.core.req.bean.HotCategory
import com.cvcxy.bigdata.spark.core.req.dao.HotCategoryAnalysisTop10Dao
import com.cvcxy.bigdata.spark.core.req.helper.HotCategoryAccumulator
import com.cvcxy.summer.framework.util.EnvUtil
import com.cvcxy.summer.framework.core.TService
import com.cvcxy.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class HotCategoryAnalysisTop10Service extends TService{

    private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao
    /**
     * 统计并关联点击、订单、支付次数
     */
    def joinClickOrderPay() = {
        // 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao
          .readFile("input/user_visit_action.txt")
        // 对品类进行点击的统计
        val categoryIdToClickCountRDD = actionRDD
          .map(
              action => {
                  val datas = action.split("_")
                  (datas(6), 1)
              }
          ).filter( _._1 != "-1" )
          .reduceByKey(_+_)
        // 对品类进行下单的统计
        val categoryIdToOrderCountRDD = actionRDD
          .map(
              action => {
                  val datas = action.split("_")
                  datas(8)
              }
          ).filter( _ != "null" )
          .flatMap{
              id => {
                  val ids = id.split(",")
                  ids.map( id=>(id,1) )
              }
          }.reduceByKey(_+_)
        // 对品类进行支付的统计
        val categoryIdToPayCountRDD = actionRDD
          .map(
              action => {
                  val datas = action.split("_")
                  datas(10)
              }
          ).filter( _ != "null" )
          .flatMap{
              id => {
                  val ids = id.split(",")
                  ids.map( id=>(id,1) )
              }
          }
          .reduceByKey(_+_)
        // join ( 品类， （点击数量，下单数量，支付数量） )
        val result= categoryIdToClickCountRDD
          .join( categoryIdToOrderCountRDD )
          .join( categoryIdToPayCountRDD )
          .mapValues {
              case ((clickCount, orderCount), payCount) => {
                  (clickCount, orderCount, payCount)
              }
          }.sortBy(_._2, false)
          .take(10)
        result
    }
    /**
     * 优化1：
     * 1.数据源加缓存，提高执行效率
     * 2.使用将数据结构转为，（点击次数，下单次数，支付次数）
     * 使用reduceByKey代替join，提高执行效率
     */
    def joinClickOrderPayOptimization() = {
        // 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao
          .readFile("input/user_visit_action.txt")
          .cache()
        // 对品类进行点击的统计
        val categoryIdToClickCountRDD = actionRDD
          .map(action => {
              val datas = action.split("_")
              (datas(6), 1)
          }
          ).filter( _._1 != "-1" )
          .reduceByKey(_+_)
          .map{case (id,clickCount)=>(id,(clickCount,0,0))}
        // 对品类进行下单的统计
        val categoryIdToOrderCountRDD = actionRDD
          .map(action => {
              val datas = action.split("_")
              datas(8)
          }
          ).filter( _ != "null" )
          .flatMap{
              id => {
                  val ids = id.split(",")
                  ids.map( id=>(id,1) )
              }
          }.reduceByKey(_+_)
          .map{case (id,orderCount)=>(id,(0,orderCount,0))}
        // 对品类进行支付的统计
        val categoryIdToPayCountRDD = actionRDD
          .map(
              action => {
                  val datas = action.split("_")
                  datas(10)
              }
          ).filter( _ != "null" )
          .flatMap{
              id => {
                  val ids = id.split(",")
                  ids.map( id=>(id,1) )
              }
          }
          .reduceByKey(_+_)
          .map{case (id,payCount)=>(id,(0,0,payCount))}
        //使用reduceByKey代替join
        val result = categoryIdToClickCountRDD
          .union(categoryIdToOrderCountRDD)
          .union(categoryIdToPayCountRDD)
          .reduceByKey((o, n) => {
              (o._1 + n._1, o._2 + n._2, o._3 + n._3)
          }).sortBy(_._2, false)
          .take(10)
        result
    }
    /**
     * 优化2：
     * 1.合并多次reduceByKey,提高执行效率
     */
    def joinClickOrderPayOptimization1() = {
        // 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao
          .readFile("input/user_visit_action.txt")
        // 对品类进行点击的统计
        val result = actionRDD
          .flatMap(action => {
              val datas = action.split("_")
              if (datas(6) !="-1") List((datas(6),(1,0,0)))
              else if (datas(8) != "null") datas(8).split(",").map(id=>(id,(0,1,0)))
              else if (datas(10) != "null") datas(10).split(",").map(id=>(id,(0,0,1)))
              else Nil
          }).reduceByKey((o, n) => {
            (o._1 + n._1, o._2 + n._2, o._3 + n._3)
        }).sortBy(_._2, false)
          .take(10)
        result
    }
    /**
     * 优化3：
     * 1.将tuple封装为样例类对象，提高代码可读性
     * 2.由于品类数一般不会超过百万，过滤后的数据量更小，单节点内存够用，我们使用scala的sortwith自定义排序规则，完成业务逻辑
     */
    def joinClickOrderPayOptimization2() = {
        // 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao
          .readFile("input/user_visit_action.txt")
        // 对品类进行点击的统计
        val result = actionRDD
          .flatMap(action => {
              val datas = action.split("_")
              if (datas(6) !="-1") List((datas(6),HotCategory(datas(6),1,0,0)))
              else if (datas(8) != "null") datas(8).split(",").map(id=>(id,HotCategory(datas(8),0,1,0)))
              else if (datas(10) != "null") datas(10).split(",").map(id=>(id,HotCategory(datas(10),0,0,1)))
              else Nil
          }).reduceByKey(
            (o,n)=>{
                o.clickCount+=n.clickCount
                o.orderCount+=n.orderCount
                o.payCount+=n.payCount
                o
            }
          ).collect()
            .sortWith(
                (left,right)=>{
                    val leftHC =left._2
                    val rightHC = right._2
                    if (leftHC.clickCount > rightHC.clickCount) true
                    else if(leftHC.clickCount == rightHC.clickCount){
                        if (leftHC.orderCount > rightHC.orderCount)  true
                        else if(leftHC.orderCount == rightHC.orderCount) {
                            leftHC.payCount > rightHC.payCount
                        }else false
                    } else false
                }
            )
        result
    }
    /**
     * 最终代码
     * 1.使用累加器替换reduceByKey，提高执行效率
     */
    override def analysis() = {
        // 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")
        // 对品类进行点击的统计
        //创建并注册累加器
        val acc = new HotCategoryAccumulator
        EnvUtil.getSCEnv().register(acc)
        // 数据加入累加器
        actionRDD.foreach(
            action =>{
                val datas = action.split("_")
                if (datas(6)!= "-1") acc.add(datas(6),"click")
                else if (datas(8) != "null") datas(8).split(",").foreach(id=>acc.add(id,"order"))
                else if (datas(8) != "null") datas(10).split(",").foreach(id=>acc.add(id,"pay"))
                else Nil
            }
        )
        //获取累加器值
        val result = acc.value.map(_._2)
          .toList
          .sortWith(
              (leftHC, rightHC) => {
                  if (leftHC.clickCount > rightHC.clickCount) true
                  else if (leftHC.clickCount == rightHC.clickCount) {
                      if (leftHC.orderCount > rightHC.orderCount) true
                      else if (leftHC.orderCount == rightHC.orderCount) {
                          leftHC.payCount > rightHC.payCount
                      } else false
                  } else false
              }
          ).take(10)
        result
    }




}
