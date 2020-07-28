package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.hive

import java.util

import com.cn.shool.bigdata.bigdata.common.time.TimeTranstationUtils
import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.hive.HiveConf
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-11-01 20:49
  */
object Kafka2HiveByDate {


  def main(args: Array[String]): Unit = {

    //构造ssc
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "Kafka2HiveByDate",5L,1)

    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams,false)

    //从kafka中拉取数据
    //TODO 数据里面没有年月日，分区需要，所以需要数据加工一下，添加年月日3个字段
    val mapDS:DStream[java.util.Map[String,String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc,Set("test1")).map(map=>{
      //添加一个日期字段，按天分组用
      val yyyyMMdd = TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(map.get("collect_time") + "000"))
      //2019-11-01
      val year = yyyyMMdd.substring(0,4) //年
      val month = yyyyMMdd.substring(4,6) //年
      val day = yyyyMMdd.substring(6,8) //年
      map.put("dayPartion",yyyyMMdd)
      map.put("year",year)
      map.put("month",month)
      map.put("day",day)
      map
    })

    //TODO 执行sql 首先构建HiveContext
    val sc = ssc.sparkContext
    val hiveContext = HiveConf.getHiveContext(sc)

    //TODO 构造建表语句，使用hiveContext循环遍历语句进行建立表
    HiveConfig.hiveTablePartitionSQL.foreach(map=>{
      hiveContext.sql(map._2)
    })

  //TODO 将数据实时写入到HDFS中
    HiveConfig.tables.foreach(table=>{
      //过滤出每种类型数据  通过fileter 避免shuffle
      val tableDS = mapDS.filter(map =>{table.equals(map.get("table"))})

      //分天  首先要获取所有的日期
      tableDS.foreachRDD(rdd=>{

        //一个RDD种去重之后的所有日期  汇集到driver上
        val arrayDays = rdd.map(x=>{x.get("dayPartion")}).distinct().collect()
        //
        arrayDays.foreach(date=>{

          val year = date.substring(0,4) //年
          val month = date.substring(4,6) //年
          val day = date.substring(6,8) //年

          //按日期构建DF
          //一个RDD种去重之后的所有日期  汇集到driver上
          //获取当前数据类型的Schema
          val tableSchema = HiveConfig.mapSchema.get(table)
          val schemaFields = tableSchema.fieldNames

          schemaFields.foreach(aaa=>{
            println("==========" + aaa)
          })
          //RDD转DF
          //TODO 构建rowRDD
          val rowRDD :RDD[Row] = rdd.filter(map=>{
              date.equals(map.get("dayPartion"))
          }).map(map=>{
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
          val tableHdfsPath = s"hdfs://hadoop-11:8020${HiveConfig.hdfsRoot}${table}/${year}/${month}/${day}"
          //写入
          tableDF.write.mode(SaveMode.Append).parquet(tableHdfsPath)
          //TODO 将HIVE表与HDFS目录建立关联关系
          hiveContext.sql(s"ALTER TABLE ${table} ADD IF NOT EXISTS PARTITION(year='${year}',month='${month}',day='${day}') LOCATION '${tableHdfsPath}'")

        })

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
