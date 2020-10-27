package com.cvcxy.bigdata.spark.core.req.application

import com.cvcxy.bigdata.spark.core.req.controller.WordCountController
import com.cvcxy.summer.framework.core.TApplication

object WordCountApplication extends App with TApplication{


    start( "spark" ) {

        val controller = new WordCountController
        controller.execute()
    }

}
