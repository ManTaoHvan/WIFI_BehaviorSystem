package org.apache.spark.streaming.kafka

import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  * author:
  * description:  直接连接获取 kafka数据  一旦任务挂掉 ，会重头开始消费
  *   获取spark内存中的偏移
  * Date:Created in 2019-10-16 21:38
  */
object KafkaUtilsTest888 extends Serializable with Logging{


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

    val DS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,KafkaParamsUtil.getKafkaParams("AAA"),Set("test1"))



    DS.foreachRDD(rdd =>{
      //记录偏移量
      //获取到spark中的kafka偏移
      val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetList.foreach(x=>{
        println(s"获取RDD中的kafka偏移信息${x}")
      })
      println(rdd.count())
      rdd.take(1).foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
