package com.cvcxy.bigdata.spark.core.req.application

import com.cvcxy.bigdata.spark.core.req.controller.{HotCategoryAnalysisTop10Controller, HotCategorySessionAnalysisTop10Controller}
import com.cvcxy.summer.framework.core.TApplication

object HotCategorySessionAnalysisTop10Application extends App with TApplication{

    // TODO 热门品类前10应用程序
    start("spark") {
        val controller = new HotCategorySessionAnalysisTop10Controller
        controller.execute()
    }
}
