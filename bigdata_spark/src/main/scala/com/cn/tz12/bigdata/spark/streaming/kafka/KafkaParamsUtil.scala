package com.cn.shool.bigdata.bigdata.spark.streaming.kafka

/**
  * author:
  * description:
  * Date:Created in 2019-10-16 21:52
  */
object KafkaParamsUtil {

  /**
    * 获取kafka参数
    * @return
    */
  def getKafkaParams(group:String): Map[String, String] ={
    val kafkaParams: Map[String, String] = Map[String,String](
      "metadata.broker.list"->"hadoop-13:9092",
      "auto.offset.reset"->"smallest",
      "group.id"->group,
      "refresh.leader.backoff.ms"->"200",
      "num.consumer.fetchers"->"1"
    )
    kafkaParams
  }
}
