package com.atguigu.spark.chapter10

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIHandler {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DirectAPIHandler").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.创建kafka参数信息
    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "youzong"
    )

    //4.获取上一次消费的位置信息 -> MySQL取出
    val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long](
      TopicAndPartition("direct", 0) -> 5L,
      TopicAndPartition("direct", 1) -> 2L
    )

    //5.使用DirectAPI读取Kafka数据创建流
    val kafkaDirectDStream: InputDStream[MessageAndMetadata[String, String]] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](
      ssc,
      kafkaParams,
      fromOffsets,
      (m: MessageAndMetadata[String, String]) => m)

    //6.定义空数组用于存放每个批次的offset信息
    var offsetRanges = Array.empty[OffsetRange]

    //7.获取流中的消费到的当前offset
    val kafkaDStream: DStream[MessageAndMetadata[String, String]] = kafkaDirectDStream.transform { rdd =>
      //获取当前批次中数据的offset
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //8.将每一行数据中的value取出
    val valueDStream: DStream[String] = kafkaDStream.map(_.message())

    //9.打印数据并打印offset信息
    valueDStream.foreachRDD { rdd =>
      //打印当前批次的数据
      rdd.foreach(println)
      //打印数据中的offset信息 -> 写入MySQL
      for (o <- offsetRanges) {
        println(s"${o.topic}->${o.partition}->${o.fromOffset}->${o.untilOffset}")
      }
    }

    //10.启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}
