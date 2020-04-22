package com.atguigu.spark.chapter08

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/3/20
  * Desc: 从Kafka数据源读取数据创建DS，高级API方式2
  * 可以将消费的偏移量保存到checkpoint中，可以从上次消费的位置重新进行消费
  */
object Streaming01_Kafka_High2 {
  def main(args: Array[String]): Unit = {
    var cp:String = "D:\\workspace\\bigdata-1021\\spark1021\\cp"
    val ssc: StreamingContext = StreamingContext
      .getActiveOrCreate(cp, () => {
        //创建配置文件对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming01_Kafka_High2")
        //创建StreamingContext对象
        val ssc = new StreamingContext(conf, Seconds(3))
        //设置checkpoint
        ssc.checkpoint(cp)

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

        //读取kafka中的数据，创建DS
        val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, Set(topic)
        )

        //wordCount
        val resDS: DStream[(String, Int)] = kafkaDS.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        //打印输出
        resDS.print()
        ssc
      })
    //开启采集器
    ssc.start()

    //等到程序终止 采集器停止
    ssc.awaitTermination()
  }
}
