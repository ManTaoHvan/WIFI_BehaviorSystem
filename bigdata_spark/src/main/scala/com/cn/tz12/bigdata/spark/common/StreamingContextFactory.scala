package com.cn.shool.bigdata.bigdata.spark.common

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}

/**
  * author:
  * description:
  * Date:Created in 2019-10-19 20:29
  */
object StreamingContextFactory  extends Serializable with Logging{


  /**
    *
    * @param appName
    * @param batchInterval
    * @param threads
    * @return
    */
  def newSparkLocalStreamingContext(appName:String="deafult",
                                    batchInterval:Long =10L,
                                    threads:Int = 1): StreamingContext ={
    val sparkConf = SparkConfFactory.newSparkLocalConf(appName,threads)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","100")
     new StreamingContext(sparkConf,Seconds(batchInterval))
  }

}
