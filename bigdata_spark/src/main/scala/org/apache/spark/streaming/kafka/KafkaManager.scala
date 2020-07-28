package org.apache.spark.streaming.kafka

import com.alibaba.fastjson.TypeReference
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

import scala.reflect.ClassTag


/**
  * author:
  * description:
  * Date:Created in 2019-10-16 22:10
  */
class KafkaManager(val kafkaParams:Map[String,String],
                   val autoUpdateoffset:Boolean=true) extends Serializable with Logging{

  //构建KafkaCluster实例
  val kafkaCluster = new KafkaCluster(kafkaParams)

/*
  val DS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    ssc,KafkaParamsUtil.getKafkaParams("AAA"),Set("test1"))
*/

  //统一数据出口
  def creatJsonToMapStringDricetStreamWithOffset(ssc: StreamingContext,
                                                 topicSet: Set[String]): DStream[java.util.Map[String,String]] ={

      //将json转为map 函数
      val converter = {
           json:String =>{
             var res :java.util.Map[String,String] = null
             try
               res = com.alibaba.fastjson.JSON.parseObject(json, new TypeReference[java.util.Map[String,String]](){})
             catch {
               case e => logError("json转map失败",e)
             }
             res
           }
      }
    createDirectStream[String,String,StringDecoder,StringDecoder](ssc,topicSet).map(map => converter(map._2))
  }


  def createDirectStream[K: ClassTag,
                          V: ClassTag,
                          KD <: Decoder[K]: ClassTag,
                          VD <: Decoder[V]: ClassTag] (
    ssc: StreamingContext,
    topics: Set[String]
  ): InputDStream[(K, V)] ={

    //TODO 从kafka读取数据之前。判断是否过期
    val groupId = kafkaParams.get("group.od").getOrElse("default")
    setOrUpdateOffsets(topics: Set[String], groupId: String)


    val messages :InputDStream[(K,V)]={

      //TODO 获取topic的所有分区
      val partitionE:Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set("test1"))
      val right = partitionE.right
      val left = partitionE.left
      //断言
      require(partitionE.isRight,s"获取partitions失败")
      val partitions:Set[TopicAndPartition]= partitionE.right.get
      println("打印分区信息")
      partitions.foreach(println(_))

      //TODO  使用kafkaCluster 从 zk中 获取消费者组的偏移 。获取消费者组的offset
      val ConsumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId,partitions)
      val right1 = ConsumerOffsetsE.right
      val left1 = ConsumerOffsetsE.left
      //断言
      require(ConsumerOffsetsE.isRight,s"获取ConsumerOffsets失败")
      val consumerOffsets:Map[TopicAndPartition,Long]= ConsumerOffsetsE.right.get
      println("打印消费者组偏移")

      //TODO 这里的消费就是从zk中获取的偏移
      KafkaUtils.createDirectStream[K,V,KD,VD,(K,V)](
        ssc,kafkaParams,consumerOffsets,(mmd: MessageAndMetadata[K, V])=>(mmd.key(),mmd.message())
      )
    }


    //TODO 将spark中的消费的偏移记录到zk中
    //默认为更新
    if(autoUpdateoffset){
      //更新zk
      messages.foreachRDD(rdd=>{
          println("RDD消费成功，卡是更新ZK")
        updateZKOffsets(rdd)
      })
    }
    // 如果
    messages
  }



  //包消费者组的offset更新到zk中
  def updateZKOffsets[K:ClassTag,V:ClassTag](rdd:RDD[(K,V)]): Unit ={

        val groupId = kafkaParams.get("group.id").getOrElse("default")
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetsList.foreach(x=>{
          println("获取spark中的偏移信息" + x)
        })

        offsetsList.foreach(offset=>{
            val topicAndPartition = TopicAndPartition(offset.topic,offset.partition)
            //将topicAndPartition 保存到zk
          setOffsets(groupId,Map((topicAndPartition,offset.untilOffset)))
        })

  }

  def setOffsets(groupId: String,
                 offsets: Map[TopicAndPartition, Long]): Unit ={
      if(offsets.nonEmpty){
         val value = kafkaCluster.setConsumerOffsets(groupId,offsets)
          if(value.isLeft){
              logError("异常")
          }
      }
  }


  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics
    * @param groupId
    */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {

      //获取kafka  partions的节点信息
      val partitionsE = kafkaCluster.getPartitions(Set(topic))
      logInfo(partitionsE+"")
      //检测
      require(partitionsE.isRight, s"获取 kafka topic ${topic}`s partition 失败。")
      val partitions = partitionsE.right.get

      //获取最早的 partions offsets信息
      val earliestLeader = kafkaCluster.getEarliestLeaderOffsets(partitions)
      val earliestLeaderOffsets = earliestLeader.right.get
      println("kafka中最早的消息偏移")
      earliestLeaderOffsets.foreach(println(_))


      //获取最末的 partions offsets信息
      val latestLeader = kafkaCluster.getLatestLeaderOffsets(partitions)
      val latestLeaderOffsets = latestLeader.right.get
      println("kafka中最末的消息偏移")
      latestLeaderOffsets.foreach(println(_))

      //获取消费者组的 offsets信息
      val consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId, partitions)
      //如果消费者offset存在
      if (consumerOffsetsE.isRight) {
        /**
          * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        //如果earliestLeader 存在
        if(earliestLeader.isRight) {
          //获取最早的offset 也就是最小的offset
          val earliestLeaderOffsets = earliestLeader.right.get
          //获取消费者组的offset
          val consumerOffsets = consumerOffsetsE.right.get
          // 将 consumerOffsets 和 earliestLeaderOffsets 的offsets 做比较
          // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
          var offsets: Map[TopicAndPartition, Long] = Map()

          consumerOffsets.foreach({ case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            //如果消費者的偏移小于 kafka中最早的offset,那麽，將最早的offset更新到zk
            if (n < earliestLeaderOffset) {
              logWarning("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
                " offsets已经过时，更新为" + earliestLeaderOffset)
              offsets += (tp -> earliestLeaderOffset)
            }
          })
          //设置offsets
          setOffsets(groupId, offsets)
        }
      } else {
        // 消费者还没有消费过  也就是zookeeper中还没有消费者的信息
        if(earliestLeader.isLeft)
          logError(s"${topic} hasConsumed but earliestLeaderOffsets is null。")
        //看是从头消费还是从末开始消费  smallest表示从头开始消费
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase).getOrElse("smallest")
        //构建消费者 偏移
        var leaderOffsets: Map[TopicAndPartition, Long] = Map.empty
        //从头消费
        if (reset.equals("smallest")) {
          //分为 存在 和 不存在 最早的消费记录 两种情况
          //如果kafka 最小偏移存在，则将消费者偏移设置为和kafka偏移一样
          if(earliestLeader.isRight){
            leaderOffsets = earliestLeader.right.get.map {
              case (tp, offset) => (tp, offset.offset)
            }
          }else{
            // 如果不存在，则从新构建偏移全部为0 offsets
            leaderOffsets = partitions.map(tp => (tp, 0L)).toMap
          }
        } else {
          //直接获取最新的offset
          leaderOffsets = kafkaCluster.getLatestLeaderOffsets(partitions).right.get.map {
            case (tp, offset) => (tp, offset.offset)
          }
        }
        //设置offsets
        setOffsets(groupId, leaderOffsets)
      }
    })
  }


}
