package com.cvcxy.bigdata.spark.core.req.controller

import com.cvcxy.bigdata.spark.core.req.service.WordCountService
import com.cvcxy.summer.framework.core.TController

/**
  * WordCount控制器
  */
class WordCountController extends TController{

    private val wordCountService = new WordCountService

    override def execute(): Unit = {
        val wordCountArray: Array[(String, Int)] = wordCountService.analysis()
        println(wordCountArray.mkString(","))
    }
}
