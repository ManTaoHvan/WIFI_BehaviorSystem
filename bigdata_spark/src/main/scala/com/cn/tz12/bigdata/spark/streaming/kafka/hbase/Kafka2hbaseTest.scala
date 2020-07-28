package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.hbase

import java.util

import com.cn.shool.bigdata.bigdata.common.time.TimeTranstationUtils
import com.cn.shool.bigdata.bigdata.hbase.config.HBaseTableUtil
import com.cn.shool.bigdata.bigdata.hbase.insert.HBaseInsertHelper
import com.cn.shool.bigdata.bigdata.hbase.spilt.SpiltRegionUtil
import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-11-02 22:01
  */
object Kafka2hbaseTest extends Serializable with Logging{

  def main(args: Array[String]): Unit = {

    //初始化hbase表
    val hbase_table = "test:test2"
    HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct)


    //构造ssc
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "Kafka2hbaseTest",5L,1)
    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams,false)

    //从kafka中拉取数据
    val mapDS:DStream[java.util.Map[String,String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc,Set("test1"))


    mapDS.foreachRDD(rdd=>{
       rdd.take(2).foreach(println(_))

       val rddPut:RDD[Put] =  rdd.map(map=>{
         val rowkey = map.get("id").toString
         val put = new Put(rowkey.getBytes())
         val keySet = map.keySet()
         keySet.foreach(key=>{
           put.addColumn("cf".getBytes(),Bytes.toBytes(key),Bytes.toBytes(map.get(key)))
         })
         put
       })

      rddPut.foreachPartition(partion=>{
         //将partion转list
         val list = new util.ArrayList[Put]
         while (partion.hasNext){
           list.add(partion.next())
         }
        HBaseInsertHelper.put(hbase_table,list)
      })
    })



    ssc.start()
    ssc.awaitTermination()


  }
}
