package com.atguigu.spark.chapter09

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/3/20
  * Desc:
  * 1.Kakfa内部早期偏移量记录在ZK上，后来版本记录在Topic__consumer_offset
  *
  * 2.SparkStreaming读取Kafka数据，创建DS，使用Kafka自带的Topic维护偏移量
  * -默认情况下，直接使用KafkaUtils读取数据，只会读取最新的数据。
  * AUTO_OFFSET_RESET_CONFIG---"largest"
  * -默认情况下，只要读到数据，就消费成功，更新偏移量
  * ENABLE_AUTO_COMMIT_CONFIG, true
  *
  *
  * 3.要向从上一次消费的位置开始消费，那么需要记录偏移量
  * 以下方式记录偏移量到检查点中
  * 	StreamingContext.getActiveOrCreate(cp,()=>StreamingContext)
  *
  * 4.如果将偏移量记录在检查点中，会存在以下问题
  * -检查点一般创建在HDFS上，如果记录偏移量会有很多的小文件产生
  * -检查点处理记录了我们偏移量之外，还记录DS的数据，如果在DS业务发生变化，
  * 在从检查点恢复数据的过程中，会出现序列化问题。
  * -如果出现序列化问题，可以将检查点删除掉，但是这就失去了记录偏移量意义
  *
  * 5.一般在开发中，大多数情况我们将偏移量维护在ZK中
  * -可以通过zk的客户端直观的看某一个Topic的某一个分区的偏移量
  * -可以自己手动的控制，什么情况下，去更新偏移量
  */
object Streaming02_Kafka_Low {
  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")

    //创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "my-bak"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //定义Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    //创建KafkaCluster（维护offset）
    val kafkaCluster = new KafkaCluster(kafkaPara)

    //获取ZK中保存的offset
    val fromOffset: Map[TopicAndPartition, Long] = getOffsetFromZookeeper(kafkaCluster, group, Set(topic))

    //读取kafka数据创建DStream
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
      kafkaPara,
      fromOffset,
      (x: MessageAndMetadata[String, String]) => x.message())

    //数据处理
    kafkaDStream.print

    //提交offset
    offsetToZookeeper(kafkaDStream, kafkaCluster, group)

    ssc.start()
    ssc.awaitTermination()
  }

  //从ZK获取offset
  def getOffsetFromZookeeper(kafkaCluster: KafkaCluster, kafkaGroup: String, kafkaTopicSet: Set[String]): Map[TopicAndPartition, Long] = {

    // 创建Map存储Topic和分区对应的offset
    val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]()

    // 获取传入的Topic的所有分区
    // Either[Err, Set[TopicAndPartition]]  : Left(Err)   Right[Set[TopicAndPartition]]
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(kafkaTopicSet)

    // 如果成功获取到Topic所有分区
    // topicAndPartitions: Set[TopicAndPartition]
    if (topicAndPartitions.isRight) {
      // 获取分区数据
      // partitions: Set[TopicAndPartition]
      val partitions: Set[TopicAndPartition] = topicAndPartitions.right.get

      // 获取指定分区的offset
      // offsetInfo: Either[Err, Map[TopicAndPartition, Long]]
      // Left[Err]  Right[Map[TopicAndPartition, Long]]
      val offsetInfo: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(kafkaGroup, partitions)

      if (offsetInfo.isLeft) {

        // 如果没有offset信息则存储0
        // partitions: Set[TopicAndPartition]
        for (top <- partitions)
          topicPartitionOffsetMap += (top -> 0L)
      } else {

        // 如果有offset信息则存储offset
        // offsets: Map[TopicAndPartition, Long]
        val offsets: Map[TopicAndPartition, Long] = offsetInfo.right.get
        for ((top, offset) <- offsets)
          topicPartitionOffsetMap += (top -> offset)
      }
    }
    topicPartitionOffsetMap.toMap
  }

  //提交offset
  def offsetToZookeeper(kafkaDstream: InputDStream[String], kafkaCluster: KafkaCluster, kafka_group: String): Unit = {
    kafkaDstream.foreachRDD {
      rdd =>
        // 获取DStream中的offset信息
        // offsetsList: Array[OffsetRange]
        // OffsetRange: topic partition fromoffset untiloffset
        val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 遍历每一个offset信息，并更新Zookeeper中的元数据
        // OffsetRange: topic partition fromoffset untiloffset
        for (offsets <- offsetsList) {
          val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
          // ack: Either[Err, Map[TopicAndPartition, Short]]
          // Left[Err]
          // Right[Map[TopicAndPartition, Short]]
          val ack: Either[Err, Map[TopicAndPartition, Short]] = kafkaCluster.setConsumerOffsets(kafka_group, Map((topicAndPartition, offsets.untilOffset)))
          if (ack.isLeft) {
            println(s"Error updating the offset to Kafka cluster: ${ack.left.get}")
          } else {
            println(s"update the offset to Kafka cluster: ${offsets.untilOffset} successfully")
          }
        }
    }
  }

}
