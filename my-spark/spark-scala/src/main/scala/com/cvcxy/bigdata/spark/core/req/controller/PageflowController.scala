package com.cvcxy.bigdata.spark.core.req.controller

import com.cvcxy.bigdata.spark.core.req.service.PageflowService
import com.cvcxy.summer.framework.core.TController

class PageflowController extends TController{

    private val pageflowService = new PageflowService

    override def execute(): Unit = {
        val result = pageflowService.analysis()
    }
}
