package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.kafka2es

import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.{ESparamsUtil, KafkaParamsUtil}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * author:
  * description:
  * Date:Created in 2019-10-19 20:23
  */
object Kafka2esStreamingTest extends Serializable with Logging{

  def main(args: Array[String]): Unit = {

    //构造ssc
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "Kafka2esStreamingTest0",5L,1)

    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams,false)

    //从kafka中拉取数据
    val mapDS:DStream[java.util.Map[String,String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc,Set("test1"))

      mapDS.foreachRDD(rdd=>{
        EsSpark.saveToEs(rdd,"test/test",ESparamsUtil.getESparams("id"))
    })

    mapDS.foreachRDD(rdd=>{
      rdd.foreach(map=>{
        EsSpark.saveToEs(rdd,"test/test",ESparamsUtil.getESparams("id"))
      })

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
