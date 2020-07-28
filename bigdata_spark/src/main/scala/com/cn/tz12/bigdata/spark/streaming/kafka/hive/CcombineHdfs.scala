package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.hive

import com.cn.shool.bigdata.bigdata.spark.common.SparkContextFactory
import com.cn.shool.bigdata.bigdata.spark.hive.HdfsAdmin
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-11-01 20:16
  */
object CcombineHdfs {

  def main(args: Array[String]): Unit = {

    //TODO 1.构建sqlContext
    val sparkContext= SparkContextFactory.newSparkLocalBatchContext("CcombineHdfs",1)
    val sqlContext = new SQLContext(sparkContext)
    //TODO 读取HDFS中的所有小文件
    HiveConfig.tables.foreach(table=>{
       println(s"==================开始合并${table}============================")
       //定义hdfs路径
       val table_path = s"hdfs://hadoop-11:8020${HiveConfig.hdfsRoot}${table}"
       val tableDF = sqlContext.read.load(table_path)
       tableDF.show(2)
       //先获取所有的小文件名，再写入原目录，最后根据小文件名删除所有小文件
      val fileSystem:FileSystem = HdfsAdmin.get().getFs
      val arrayFileStatuses:Array[FileStatus] = fileSystem.globStatus(new Path(table_path+"/part*"))
      //将文件状态转为path
      val paths:Array[Path] =  FileUtil.stat2Paths(arrayFileStatuses)
      //TODO 合并小文件，从新写入原目录，
      tableDF.repartition(1).write.mode(SaveMode.Append).parquet(table_path)
      //TODO 删除原来的小文件
      paths.foreach(path=>{
        fileSystem.delete(path)
        println(s"删除小文件${path}成功")
      })
    })

  }

}
