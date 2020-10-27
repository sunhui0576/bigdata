package com.cvcxy.bigdata.spark.core.req.application

import com.cvcxy.bigdata.spark.core.req.controller.PageflowController
import com.cvcxy.summer.framework.core.TApplication

object PageflowApplication extends App with TApplication{

    start( "spark" ) {
        val controller = new PageflowController
        controller.execute()
    }
}
