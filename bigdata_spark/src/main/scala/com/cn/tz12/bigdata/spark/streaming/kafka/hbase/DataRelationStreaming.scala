package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.hbase

import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil
import com.cn.shool.bigdata.bigdata.hbase.config.HBaseTableUtil
import com.cn.shool.bigdata.bigdata.hbase.insert.HBaseInsertHelper
import com.cn.shool.bigdata.bigdata.hbase.spilt.SpiltRegionUtil
import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * author:
  * description:
  * Date:Created in 2019-11-04 20:03
  */
object DataRelationStreaming extends Serializable with Logging {

  var relationFields : Array[String] = null
  def main(args: Array[String]): Unit = {

    val relationPath = "test/spark/hive/relation.properties"
    relationFields = ConfigUtil.getInstance().getProperties(relationPath).getProperty("relationfield").split(",")
    relationFields.foreach(println(_))


    initRelationTables
    //构造ssc
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "Kafka2hbaseTest", 5L, 1)
    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams, false)

    //从kafka中拉取数据
    val mapDS: DStream[java.util.Map[String, String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc, Set("test1"))

    mapDS.foreachRDD(rdd => {
      rdd.foreachPartition(partion => {
        //因为需要一条一条关联，所以不能批量操作
        while (partion.hasNext) {
          //TODO 首先往主关联表里面写入数据
          val map = partion.next()
          relationFields.foreach(relationField => {
            //判断这个字段是不是在map里面，如果在，才写入，不在不写
            if (map.containsKey(relationField)) {
              //TODO 构建表名
              val tableName = "test:relation"
              //TODO 构建put
              //构建主关联表rowkey
              val phone_mac = map.get("phone_mac")
              val put = new Put(phone_mac.getBytes())
              //添加列
              //public Put addColumn(byte [] family, byte [] qualifier, long ts, byte [] value)
              val family = "cf".getBytes()
              val qualifier = relationField.getBytes()
              val fieldValue = map.get(relationField).getBytes()
              //版本号使用 字段+字段值 取hashcode 取正
              val ts = (relationField + fieldValue).hashCode & Integer.MAX_VALUE
              put.addColumn(family,qualifier,ts,fieldValue)
              HBaseInsertHelper.put(tableName,put)

              //TODO  构建倒排索引表
              //TODO 构建表名
              val sort_TableName = s"test:${relationField}"
              //TODO 构建put
              val sort_put = new Put(fieldValue)
              val sort_family = "cf".getBytes()
              val sort_qualifier = "phone_mac".getBytes()
              val sort_fieldValue = phone_mac.getBytes()
              //版本号使用 字段值 取hashcode 取正
              val sort_ts = (phone_mac).hashCode & Integer.MAX_VALUE
              sort_put.addColumn(sort_family,sort_qualifier,sort_ts,sort_fieldValue)
              HBaseInsertHelper.put(sort_TableName,sort_put)
            }
          })
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 初始化hbase
    */
  def initRelationTables(): Unit ={
      //主关联表
      val tableName = "test:relation"
    HBaseTableUtil.createTable(tableName, "cf", true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
     //倒排索引表
     relationFields.foreach(field=>{
      val hbase_table = s"test:${field}"
      HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
    })
  }


}
