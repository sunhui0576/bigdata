package com.cvcxy.bigdata.spark.core.req.service

import com.cvcxy.bigdata.spark.core.req.dao.WordCountDao
import com.cvcxy.summer.framework.util.EnvUtil
import com.cvcxy.summer.framework.core.TService
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService extends TService {

    private val wordCountDao = new WordCountDao

    /**
      * 数据分析
      *
      * @return
      */
    override def analysis() = {
        // EnvUtil.getSCEnv().textFile("input/word.txt")
        val fileRDD: RDD[String] = wordCountDao.readFile("input/word.txt")
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = wordRDD.map( word=>(word,1) )
        val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        val wordCountArray: Array[(String, Int)] = wordToSumRDD.collect()
        wordCountArray
    }
}
