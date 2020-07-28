package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

/**
  * author:
  * description: 获取kafka 本身的偏移
  * Date:Created in 2019-10-16 20:18
  */
object KafkaClusterTest {

  val kafkaParams: Map[String, String] = Map[String,String](
    "metadata.broker.list"->"hadoop-13:9092",
    "auto.offset.reset"->"smallest",
    "group.id"->"test1",
    "refresh.leader.backoff.ms"->"200",
    "num.consumer.fetchers"->"1"
  )
  def main(args: Array[String]): Unit = {


    val kafkaCluster = new KafkaCluster(kafkaParams)


    //TODO 获取topic的所有分区
    val partitionE:Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set("test1"))
    val right = partitionE.right
    val left = partitionE.left
    //断言
    require(partitionE.isRight,s"获取partitions失败")
    val partitions:Set[TopicAndPartition]= partitionE.right.get
    println("打印分区信息")
    partitions.foreach(println(_))

    //TODO 获取最早偏移
    val earliestLeaderOffsetsE: Either[Err, Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.getEarliestLeaderOffsets(partitions)
    val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
    println("打印最早的偏移")
    earliestLeaderOffsets.foreach(println(_))


    //TODO 获取最末偏移
    val latestLeaderOffsetsE: Either[Err, Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.getLatestLeaderOffsets(partitions)
    //断言
    require(latestLeaderOffsetsE.isRight,s"获取partitions失败")
    val latestLeaderOffsets = latestLeaderOffsetsE.right.get
    println("打印最末的偏移")
    latestLeaderOffsets.foreach(println(_))

    //TODO 获取消费者组的偏移

    val ConsumerOffsetsE = kafkaCluster.getConsumerOffsets("AAA",partitions)
    val right1 = ConsumerOffsetsE.right
    val left1 = ConsumerOffsetsE.left
    println(right1)
    println(left1)
    //断言
    require(ConsumerOffsetsE.isRight,s"获取ConsumerOffsets失败")
    val consumerOffsets = ConsumerOffsetsE.right.get
    println("打印消费者组偏移")
    consumerOffsets.foreach(println(_))
  }

}
