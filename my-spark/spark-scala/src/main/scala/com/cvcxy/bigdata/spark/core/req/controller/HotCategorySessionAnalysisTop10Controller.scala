package com.cvcxy.bigdata.spark.core.req.controller

import com.cvcxy.bigdata.spark.core.req.bean
import com.cvcxy.bigdata.spark.core.req.service.{HotCategoryAnalysisTop10Service, HotCategorySessionAnalysisTop10Service}
import com.cvcxy.summer.framework.core.TController

class HotCategorySessionAnalysisTop10Controller extends TController {

    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
    private val hotCategorySessionAnalysisTop10Service = new HotCategorySessionAnalysisTop10Service

    override def execute(): Unit = {
        val categories: List[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis()
        val result = hotCategorySessionAnalysisTop10Service.analysis(categories)
        result.foreach(println)
    }
}
