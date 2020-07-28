package com.cn.shool.bigdata.bigdata.spark.streaming.kafka.warn

import java.util.Timer

import com.cn.shool.bigdata.bigdata.common.time.TimeTranstationUtils
import com.cn.shool.bigdata.bigdata.redis.client.JedisUtil
import com.cn.shool.bigdata.bigdata.spark.common.StreamingContextFactory
import com.cn.shool.bigdata.bigdata.spark.streaming.kafka.KafkaParamsUtil
import com.cn.shool.bigdata.bigdata.spark.warn.dao.WarningMessageDao
import com.cn.shool.bigdata.bigdata.spark.warn.domain.WarningMessage
import com.cn.shool.bigdata.bigdata.spark.warn.timer.{PhoneWarnImpl, SyncRule2RedisTimer, WarnI}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import redis.clients.jedis.Jedis

/**
  * author:
  * description:
  * Date:Created in 2019-10-23 20:48
  */
object WarnStreamingTask extends Serializable with Logging{

  def main(args: Array[String]): Unit = {

    //TODO 使用定时器将mysql中得规则同步到redis
    val timer: Timer = new Timer
    timer.schedule(new SyncRule2RedisTimer, 0, 1 * 30 * 1000)

    //TODO 读取kafka流数据
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext(
      "Kafka2esStreaming",5L,1)
    //构造kafkaManager
    val kafkaParams = KafkaParamsUtil.getKafkaParams("AAA")
    val kafkaManager = new KafkaManager(kafkaParams,false)
    //从kafka中拉取数据
    val mapDS:DStream[java.util.Map[String,String]] = kafkaManager.creatJsonToMapStringDricetStreamWithOffset(
      ssc,Set("test1"))

    val array = Array("phone")
    //TODO 数据处理
    mapDS.foreachRDD(rdd=>{
       //序列化问题
      rdd.foreachPartition(partion=>{
          val jedis14:Jedis = JedisUtil.getJedis(14)   //设备缓存
          val jedis15:Jedis = JedisUtil.getJedis(15)    //规则缓存
         //遍历partion
        while (partion.hasNext){
          val map = partion.next()
          //拿到数据之后怎么进行比对
          array.foreach(field=>{
              //如果map中含有预警字段 进行比对预警
              if(map.containsKey(field)){
                 //拼接redisKey
                 val fieldValue = map.get(field)
                 val redisKey = field + ":" + fieldValue
                  //判断redis中是否存在这个key,如果存在说明命中了
                 val boolean = jedis15.exists(redisKey)
                 if(boolean){
                    //TODO 命中 告警
                   //时间间隔控制
                    val warn_time = jedis15.hget(redisKey,"warn_time")
                    if(StringUtils.isNotBlank(warn_time)){
                         //说明不是第一次告警，做判断，判断时间间隔
                        val now_time = System.currentTimeMillis()/1000
                        val redis_time = java.lang.Long.valueOf(warn_time)
                        //如果距上一次告警事件大于30秒
                        if(now_time - redis_time > 30){
                          warn(redisKey,jedis15,jedis14,map)
                          jedis15.hset(redisKey,"warn_time",System.currentTimeMillis()/1000+"")
                        }

                    }else{
                       //如果预警事件为空，说明还没有命中,是第一次预警
                      warn(redisKey,jedis15,jedis14,map)
                      // 告警完成 将当前时间刷到redis中
                      jedis15.hset(redisKey,"warn_time",System.currentTimeMillis()/1000+"")
                    }
                 }
              }
          })
        }
        JedisUtil.close(jedis14)
        JedisUtil.close(jedis15)

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 预警
    * @param redisKey
    * @param jedis15
    * @param map
    */
  def warn(redisKey:String,jedis15:Jedis,jedis14:Jedis,map:java.util.Map[String,String]): Unit ={

      //TODO 组装消息体
      val split = redisKey.split(":")

      if(split.length == 2){
        val message = new WarningMessage
        //从redis中获取所有消息内容
        val redisMap = jedis15.hgetAll(redisKey)
        message.setAlarmRuleid(redisMap.get("id")) //设置消息id
        message.setSendMobile(redisMap.get("send_mobile"))
        message.setSendType(redisMap.get("sent_type"))
        message.setAccountid(redisMap.get("publisher"))
        message.setAlarmType("2")

        //封装内容
        val warn_type = "【黑名单告警】=》"
        //设备号  经纬度
        val device_number = map.get("device_number")
        val device_address = jedis14.get(device_number)

        val longitude = map.get("longitude")
        val latitude = map.get("latitude")
        val collect_time = map.get("collect_time")
        val phone = map.get("phone")
        println("warn_fieldvalue=====================" + phone)

        val realDate = TimeTranstationUtils.Date2yyyyMMdd_HHmmss(java.lang.Long.valueOf(map.get("collect_time") + "000"))

        //val all_message = hbase.get("phone")

        val warn_content = s"${warn_type} 【手机号为${map.get("phone")}】的嫌犯在" +
          s"${realDate} 出现在经纬度【${longitude},${latitude}】的地点，具体地址为 ${device_address}"
        //TODO 将消息存到mysql数据库
        message.setSenfInfo(warn_content)
        WarningMessageDao.insertWarningMessageReturnId(message)
        //TODO 根据告警类型 发送不同告警消息
        // 发短信,调用第三方接口 发送信息

        if(message.getSendType.equals("2")){
            //短信告警
            val warnI = new PhoneWarnImpl();
            warnI.warn(message)
        }
        if(message.getSendType.equals("1")){
            //微信告警

        }


      }


  }

}
