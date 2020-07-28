package org.apache.spark.streaming.kafka

import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * author:
  * description:  直接连接获取 kafka数据  一旦任务挂掉 ，会重头开始消费
  * Date:Created in 2019-10-16 21:38
  */
object KafkaUtilsTest1 extends Serializable with Logging{


  def main(args: Array[String]): Unit = {


    /*  def createDirectStream[
    K: ClassTag,
     V: ClassTag,
     KD <: Decoder[K]: ClassTag,
     VD <: Decoder[V]: ClassTag] (
                                   ssc: StreamingContext,
                                   kafkaParams: Map[String, String],
                                   topics: Set[String]
                                 )*/

    val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaUtilsTest").setMaster("local[1]")
    //sparkConf.set("spark.streaming.receiver.maxRate","1")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(10L))

  /*  val DS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,KafkaParamsUtil.getKafkaParams("AAA"),Set("test1"))

*/
    //从zk里面获取偏移
    val kafkaManager = new KafkaManager(KafkaParamsUtil.getKafkaParams("AAA"),false)
    val DS =  kafkaManager.creatJsonToMapStringDricetStreamWithOffset(ssc,Set("test1"))

/*     val kafkaManager = new KafkaManager()
    kafkaManager.createDirectStream(String,String,,StringDecoder)()*/

    //spark使用低阶API消费kafka的时候，不会自动为我们存储zk
    DS.foreachRDD(rdd =>{
      //记录偏移量
      //获取到spark中的kafka偏
      println(rdd.count())
      rdd.foreach(x=>{
        println(x)
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
