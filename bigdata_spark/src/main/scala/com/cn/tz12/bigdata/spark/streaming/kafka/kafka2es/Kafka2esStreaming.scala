package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.kafka2es

import com.cn.shool.bigdata.bigdata.common.properties.DataTypeProperties
import com.cn.shool.bigdata.bigdata.common.time.TimeTranstationUtils
import com.cn.shool.bigdata.bigdata.es.admin.AdminUtil
import com.cn.shool.bigdata.bigdata.es.client.ESclientUtil
import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.common.convert.DataConvert
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.{ESparamsUtil, KafkaParamsUtil}
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-10-19 21:49
  */
object Kafka2esStreaming extends Serializable with Logging{

  def main(args: Array[String]): Unit = {
    //构造ssc
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "Kafka2esStreaming",5L,1)
    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams,false)

    //从kafka中拉取数据
    val mapDS:DStream[java.util.Map[String,String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc,Set("test1")).map(map=>{
       //添加一个日期字段，按天分组用
      map.put("dayPartion",TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(map.get("collect_time") + "000")))
      map
    })
    //现在的数据是3种数据全部混合在一起,按日期建立索引之前，首先要对数据进行拆分
    //使用filetr避免shuffle  先拿到所有数据类型
    val tableSet = DataTypeProperties.dataTypeMap.keySet()

    tableSet.foreach(table=>{
        //过滤出每种类型数据  通过fileter 避免shuffle
        val tableDS = mapDS.filter(map =>{table.equals(map.get("table"))})
        val client = ESclientUtil.getClient
        //分天  首先要获取所有的日期
      tableDS.foreachRDD(rdd=>{
          //一个RDD种去重之后的所有日期  汇集到driver上
          val arrayDays = rdd.map(x=>{x.get("dayPartion")}).distinct().collect()
          arrayDays.foreach(day=>{

            val index = table + "_" + day
            //TODO 判断这个索引是不是存在
            val bool = AdminUtil.indexExists(client,index)
            //存在就不用新建
            //不存在，就需要动态创建索引
            if(!bool){
              val path = s"test/es/mapping/${table}.json"
              //index: String,`type`: String,path: String,shard: Int,replication: Int
              AdminUtil.buildIndexAndTypes(index,index,path,5,1)
            }
            //按天分组
            val table_dayRDD = rdd.filter(map =>{day.equals(map.get("dayPartion"))})
                .map(map=>{
                   //将string,string 转为String,Object
                  val objectMap = DataConvert.strMap2esObjectMap(map)
                  objectMap
                })
            println("table_dayRDD=========" + table_dayRDD.count())
            EsSpark.saveToEs(table_dayRDD,s"${index}/${index}",ESparamsUtil.getESparams("id"))
          })
      })


    })
    ssc.start()
    ssc.awaitTermination()
  }
}
