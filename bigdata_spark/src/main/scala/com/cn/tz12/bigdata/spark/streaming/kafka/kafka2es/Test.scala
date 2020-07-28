package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.kafka2es

import com.cn.shool.bigdata.bigdata.common.time.TimeTranstationUtils

/**
  * @author:
  * @description:
  * @Date:Created in 2019-10-
  */
object Test {
  def main(args: Array[String]): Unit = {
    val str = TimeTranstationUtils.Date2yyyyMMdd("1257305988")
    print(str)
  }
}
