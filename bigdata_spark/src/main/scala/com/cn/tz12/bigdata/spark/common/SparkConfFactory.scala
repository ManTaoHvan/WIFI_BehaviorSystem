package com.cn.shool.bigdata.bigdata.spark.common

import org.apache.spark.{Logging, SparkConf}

/**
  * author:
  * description:
  * Date:Created in 2019-10-19 20:25
  */
object SparkConfFactory extends Serializable with Logging{


  /**
    * 离线本地批处理SparkConf
    * @param appName
    * @param threads
    */
    def newSparkLocalConf(appName:String="deafult",threads:Int = 1): SparkConf ={
      val sparkConf = new SparkConf()
      sparkConf.setAppName(appName).setMaster(s"local[$threads]")
    }



}
