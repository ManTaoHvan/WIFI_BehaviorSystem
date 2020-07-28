package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.warn

import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * author:
  * description:
  * Date:Created in 2019-08-23 21:33
  */
object WarnFlowStreamingTask extends Serializable with Logging{

      //把一 个字段 count 一下 如果大于阈值  就告警

  def main(args: Array[String]): Unit = {
    //定义topic
    val topics = "test1"
    //首先从kafka读取数据
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("WarningStreamingTask", 10L, 2)
    //val ssc = StreamingContextFactory.newSparkStreamingContext("SparkstreamingKafka2esTest",10)
    //构建kafka参数
    val kafkaParams = KafkaParamsUtil.getKafkaParams("WarningStreamingTask")
    val kafkaManager = new KafkaManager(kafkaParams, false)
    val mapDS = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(ssc, Set(topics))
      //数据加工处理，添加日期字段
      .map(map => {
        val collect_time = map.get("collect_time")
        java.lang.Long.valueOf(collect_time)
    })

    //每10秒钟的流量
    mapDS.foreachRDD(rdd=>{
      val flow10 = rdd.reduce(_+_)
      println("flow10====" + flow10)
      if(flow10 > 10000){
          println("【流量预警】流量大于10000" )
      }
    }
      //每天>1G  网络
      //每天上网时间    给他贴一个  网络大人  网瘾少年
    )
    ssc.start()
    ssc.awaitTermination()

  }

}
