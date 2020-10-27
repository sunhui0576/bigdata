package com.cvcxy.bigdata.spark.core.req.controller

import com.cvcxy.bigdata.spark.core.req.service.HotCategoryAnalysisTop10Service
import com.cvcxy.summer.framework.core.TController

class HotCategoryAnalysisTop10Controller extends TController {

    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

    override def execute(): Unit = {
        val result = hotCategoryAnalysisTop10Service.analysis()

        result.foreach(println)
    }
}
