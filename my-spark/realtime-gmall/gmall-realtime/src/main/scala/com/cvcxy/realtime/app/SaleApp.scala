package com.cvcxy.realtime.app

object SaleApp {
  /**
   * 灵活查询案例：根据条件来灵活分析用户的购买行为.
   * 双流join思路1：orderInfo join orderDetail
   * 窗口多个批次，滑动步长=一个批次，一定程度减少join数据丢失
   * 问题：滑动长度小于窗口大小，同一个批次会在多个窗口重复join，join数据会有重复
   * 去重：redis的set集合order_detail存入set
   * 存入es,document使用order_detail
   * 对于延时数据大小超过窗口大小的数据会丢失
   * 双流join思路2：
   * 同批次join，不设置窗口，将没有join上的数据存入redis缓存,30分钟后自动删除
   * 新批次数据之间join后都去缓存中查一下有没有能join上的
   * 反查hbase
*/

}
