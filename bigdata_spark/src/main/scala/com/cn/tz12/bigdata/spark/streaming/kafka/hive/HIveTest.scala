package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.hive

import java.util

import com.cn.shool.bigdata.bigdata.es.admin.AdminUtil
import com.cn.shool.bigdata.bigdata.es.client.ESclientUtil
import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.common.convert.DataConvert
import com.cn.shool.bigdata.bigdata.spark.hive.HiveConf
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.{ESparamsUtil, KafkaParamsUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-10-25 21:52
  */
object HIveTest extends Serializable with Logging{

  def main(args: Array[String]): Unit = {

    //构造ssc
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "HIveTest",5L,1)

    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams,false)

    //从kafka中拉取数据
    val mapDS:DStream[java.util.Map[String,String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc,Set("test1"))

    //TODO 执行sql 首先构建HiveContext
     val sc = ssc.sparkContext
     val hiveContext = HiveConf.getHiveContext(sc)

    //TODO 构造建表语句，使用hiveContext循环遍历语句进行建立表
    HiveConfig.hiveTableSQL.foreach(map=>{
      hiveContext.sql(map._2)
    })

    //TODO 将数据实时写入到HDFS中
    HiveConfig.tables.foreach(table=>{
      //过滤出每种类型数据  通过fileter 避免shuffle
      val tableDS = mapDS.filter(map =>{table.equals(map.get("table"))})
      //分天  首先要获取所有的日期
      tableDS.foreachRDD(rdd=>{
        //一个RDD种去重之后的所有日期  汇集到driver上
        //获取当前数据类型的Schema
        val tableSchema = HiveConfig.mapSchema.get(table)
        val schemaFields = tableSchema.fieldNames

        schemaFields.foreach(aaa=>{
           println("==========" + aaa)
        })
        //RDD转DF
       // val schema = StructType()
        val rowRDD:RDD[Row] = rdd.map(map=>{
          //把map转为Row
            val listRow = new util.ArrayList[Object]()
            //对map进行遍历
            for(schemaFields <- schemaFields){
              listRow.add(map.get(schemaFields))
            }
            Row.fromSeq(listRow)
        })
        val tableDF = hiveContext.createDataFrame(rowRDD, tableSchema)
        tableDF.show(2)
        //定义HDFS路径
        val tableHdfsPath = s"hdfs://hadoop-11:8020${HiveConfig.hdfsRoot}${table}"
        //写入
        tableDF.write.mode(SaveMode.Append).parquet(tableHdfsPath)
       //TODO 将HIVE表与HDFS目录建立关联关系
        hiveContext.sql(s"ALTER TABLE ${table} LOCATION '${tableHdfsPath}'")

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
