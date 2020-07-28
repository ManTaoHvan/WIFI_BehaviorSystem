package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.hive

import java.util

import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil
import javax.naming.ConfigurationException
import org.apache.commons.configuration.{CompositeConfiguration, PropertiesConfiguration}
import org.apache.spark.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * author:
  * description: 配置构造
  * Date:Created in 2019-10-26 20:12
  */
object HiveConfig extends Serializable with Logging {


  val hiveFilePath = "test/es/mapping/fieldmapping.properties"
  var config: CompositeConfiguration = null
  val hdfsRoot = "/user/hive/external/"

  //所有的类型
  var tables: java.util.List[_] = null

  // table => tableFields
  var tableFiledsMap: java.util.Map[String, java.util.HashMap[String, String]] = null

  //  table => tabelSQL建表语句
  var hiveTableSQL: java.util.HashMap[String, String] = null

  //  table => 创建分无标tabelSQL建表语句
  var hiveTablePartitionSQL: java.util.HashMap[String, String] = null


  // mapSchema
  var mapSchema: java.util.HashMap[String, StructType] = null


  def main(args: Array[String]): Unit = {

  }

  def initParams(): Unit = {

    //初始化配置文件
    println("=========================加载配置文件config===================")
    config = HiveConfig.readCompositeConfiguration(hiveFilePath)
    val keys = config.getKeys
    while (keys.hasNext) {
      println(keys.next())
    }

    println("=========================获取tables===================")
    tables = config.getList("tables")
    tables.foreach(table => {
      println(table)
    })

    println("=========================获取tableFiledsMap===================")
    tableFiledsMap = getFieldsByTable()
    tableFiledsMap.foreach(x => {
      println(x)
    })





    println("=========================获取建表语句===================")
    hiveTableSQL = HiveConfig.createHiveTables()
    hiveTableSQL.foreach(x => {
      println(x)
    })


    println("=========================获取分区建表语句===================")
    hiveTablePartitionSQL = HiveConfig.createHivePartitionTables()
    hiveTablePartitionSQL.foreach(x => {
      println(x)
    })

    createHivePartitionTables

    println("=========================创建RDD转DF的schema===================")
    mapSchema = createSchema()
    mapSchema.foreach(x=>{
      println(x)
    })
  }



  initParams()


  /**
    * 创建RDD转DF的Schema
    *
    * @return
    */
  def createSchema(): java.util.HashMap[String, StructType] = {
    //mapStructType 封装的3个表的StructType
    val mapStructType = new java.util.HashMap[String, StructType]
    //遍历所有的表
    for (table <- tables) {
      val arrayStructFields = ArrayBuffer[StructField]()
      //获取到表的所有字段
     /* val tableFields1 = tableFiledsMap.get(table)
      val keyIterator1 =  tableFields1.keySet().iterator()
*/
      val tableFields = config.getKeys(table.toString)
      //val keyIterator =  tableFields.keySet().iterator()

      while (tableFields.hasNext){
        //获取字段
        val key = tableFields.next()
        //获取字段类型
        val fieldType = config.getProperty(key.toString)
        val filed = key.toString.split("\\.")(1)

        //通过模式匹配构建StructField
        fieldType match {
          case "string" => arrayStructFields += StructField(filed,StringType,true)
          case "long" => arrayStructFields += StructField(filed,StringType,true)
          case "double" => arrayStructFields += StructField(filed,StringType,true)
          case _ =>
        }
      }
      val schema = StructType(arrayStructFields)
      mapStructType.put(table.toString, schema)
    }
    mapStructType
  }


  /**
    * 创建分区SQL语句  按年 月  日创建分区表
    *
    * @return
    */
  def createHivePartitionTables(): java.util.HashMap[String, String] = {

    val hiveSqlMap = new java.util.HashMap[String, String]
    tables.foreach(table => {
      //开始拼接建表语句
      //s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} (imei string,
      // absolute_filename string) STORED AS PARQUET LOCATION '/user/hive/external/${table}'"
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //拼接字段  这个方法获取到的是有序的
      val fields = config.getKeys(table.toString)

      fields.foreach(tableField => {
        //字段
        val field = tableField.toString.split("\\.")(1)
        //字段类型
        val fieldType = config.getProperty(tableField.toString)

        sql = sql + field
        //进行类型转换
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })
      //切掉最后多的一个,
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s") partitioned by (year string,month string,day string) STORED AS PARQUET LOCATION '/user/hive/external/${table}'"

      hiveSqlMap.put(table.toString, sql)
    })
    hiveSqlMap
  }




  /**
    * 创建SQL语句
    *
    * @return
    */
  def createHiveTables(): java.util.HashMap[String, String] = {

    val hiveSqlMap = new java.util.HashMap[String, String]
    tables.foreach(table => {
      //开始拼接建表语句
      //s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} (imei string,
      // absolute_filename string) STORED AS PARQUET LOCATION '/user/hive/external/${table}'"
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //拼接字段  这个方法获取到的是有序的
      val fields = config.getKeys(table.toString)

      fields.foreach(tableField => {
        //字段
        val field = tableField.toString.split("\\.")(1)
        //字段类型
        val fieldType = config.getProperty(tableField.toString)

        sql = sql + field
        //进行类型转换
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })
      //切掉最后多的一个,
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s") STORED AS PARQUET LOCATION '/user/hive/external/${table}'"

      hiveSqlMap.put(table.toString, sql)
    })
    hiveSqlMap
  }


  def getFieldsByTable(): java.util.Map[String, java.util.HashMap[String, String]] = {

    val map = new java.util.HashMap[String, java.util.HashMap[String, String]]()
    //遍历所有的表名
    val iteratorTables = tables.iterator()
    while (iteratorTables.hasNext) {
      val tableMap = new util.HashMap[String, String]()
      val table = iteratorTables.next().toString
      //通过前缀匹配所有字段
      val fields = config.getKeys(table)
      while (fields.hasNext) {
        val filed = fields.next().toString
        tableMap.put(filed, config.getString(filed))
      }
      map.put(table, tableMap)
    }
    map
  }




  /**
    * 读取配置文件
    *
    * @param path
    * @return
    */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {
    val compositeConfiguration = new CompositeConfiguration

    try {
      val configuration = new PropertiesConfiguration(path)
      compositeConfiguration.addConfiguration(configuration)
    } catch {
      case e: ConfigurationException => {
        logError(s"加载配置文件${path}失败", e)
      }
    }
    logInfo(s"加载配置文件${path}成功")
    compositeConfiguration
  }

}
