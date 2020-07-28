package com.cn.shool.bigdata.bigdata.spark.common.convert


import java.util
import java.util.HashMap

import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil
import scala.collection.JavaConversions._

/**
  * author:
  * description: 数据类型转换
  * Date:Created in 2019-10-21 20:45
  */
object DataConvert {

  val fieldMappingPath = "test/es/mapping/fieldmapping.properties"

  private val tableFiledMap : util.HashMap[String, util.HashMap[String, String]] = getEsFieldtypeMap()

  def main(args: Array[String]): Unit = {
    val stringToStringToString = DataConvert.getEsFieldtypeMap()
    val strings = stringToStringToString.keySet()
    strings.foreach(key => {
      println(key + "===>" + stringToStringToString.get(key))
    })
  }


  def strMap2esObjectMap(map: java.util.Map[String, String]): java.util.Map[String, Object] = {

    //获取这条数据的数据类型
    val table = map.get("table")

   // qq===>{latitude=double, imsi=string, accept_message=string, phone_mac=string, message_time=long, device_mac=string, filename=string, phone=string, absolute_filename=string, device_number=string, imei=string, id=string, collect_time=long, send_message=string, table=string, object_username=string, username=string, longitude=double}
   // mail===>{mail_content=string, mail_type=string, latitude=double, imsi=string, phone_mac=string, device_mac=string, filename=string, send_time=long, absolute_filename=string, device_number=string, imei=string, send_mail=string, id=string, accept_mail=string, collect_time=long, accept_time=long, table=string, longitude=double}
   // wechat===>{latitude=double, imsi=string, accept_message=string, phone_mac=string, device_mac=string, message_time=long, filename=string, absolute_filename=string, phone=string, device_number=string, imei=string, collect_time=long, id=string, send_message=string, table=string, object_username=string, longitude=double, username=string}

    //根据类型获取映射关系
    val fieldmappingMap = tableFiledMap.get(table)
    //获取配置文件的所有字段
    //val tableKeySet = fieldmappingMap.keySet()

    //定义转换之后的map
    val objectMap = new util.HashMap[String,Object]()
    //拿到真实数据的所有字段
    val set = map.keySet().iterator()

    while (set.hasNext){
      //真实数据key
      val key = set.next()
      //真实数据的类型
      val dataType = fieldmappingMap.get(key)
      //模式匹配，进行数据类型转换
      dataType match {
        case "long" => BaseDataConvert.mapString2Long(map,key,objectMap)
        case "string" =>  BaseDataConvert.mapString2String(map,key,objectMap)
        case "double" =>  BaseDataConvert.mapString2Double(map,key,objectMap)
        case _ => BaseDataConvert.mapString2String(map,key,objectMap)
      }
    }
    objectMap
  }


  /**
    * 获取配置文件字段和字段类型的映射关系
    *
    * @return
    */
  def getEsFieldtypeMap(): HashMap[String, HashMap[String, String]] = {

    //总集合
    val mapMap = new HashMap[String, util.HashMap[String, String]]
    val properties = ConfigUtil.getInstance().getProperties(fieldMappingPath)
    //获取所有的key
    val tableFields = properties.keySet()
    //获取所有数据类型
    val tables = properties.get("tables").toString.split(",")

    tables.foreach(table => {
      //封装一种数据类型的 map
      val map = new util.HashMap[String, String]
      tableFields.foreach(tableField => {
        if (tableField.toString.startsWith(table)) {
          val key = tableField.toString.split("\\.")(1)
          val value = properties.get(tableField).toString
          map.put(key, value)
        }
      })
      mapMap.put(table, map)
    })
    mapMap
  }


}
