package com.cn.shool.bigdata.bigdata.spark.common

import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * @author:
  * @description:
  * @Date:Created in 2019-10-
  */
object SparkContextFactory  extends Serializable with Logging{

  /**
    * 离线本地批处理SparkContext
    * @param appName
    * @param threads
    */
  def newSparkLocalBatchContext(appName:String="deafult",threads:Int = 1): SparkContext ={
    val sparkConf = SparkConfFactory.newSparkLocalConf(appName,threads)
    new SparkContext(sparkConf)
  }


}
