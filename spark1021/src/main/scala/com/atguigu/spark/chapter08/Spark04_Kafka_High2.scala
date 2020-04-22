package com.atguigu.spark.chapter08

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark04_Kafka_High2 {

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("D:\\workspace\\bigdata-1021\\spark1021\\cp", () => createSSC())
    ssc.start()
    ssc.awaitTermination()
  }
  def createSSC(): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 偏移量保存在 checkpoint 中, 可以从上次的位置接着消费
    ssc.checkpoint("D:\\workspace\\bigdata-1021\\spark1021\\cp")
    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "my-bak"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //创建DS
    val kafkaDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaParams, Set(topic))
    //wordCount
    val resDS: DStream[(String, Int)] = kafkaDS.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //打印输出
    resDS.print()
    ssc
  }

}
