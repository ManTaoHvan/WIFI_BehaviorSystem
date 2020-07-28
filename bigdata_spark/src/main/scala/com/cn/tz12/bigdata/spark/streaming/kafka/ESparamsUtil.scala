package com.cn.shool.bigdata.bigdata.spark.streaming.kafka

/**
  * author:
  * description: ES配置
  * Date:Created in 2019-10-19 20:41
  */
object ESparamsUtil {



      def getESparams(id_field:String): Map[String,String] ={
         Map[String,String](
          "es.mapping.id"->id_field,  //用map中的哪个字段作为ES主键
          "es.nodes"->"hadoop-11,hadoop-12,hadoop-13",
          "es.port"->"9200",
          "es.clustername"->"my-application"
        )
      }

}
