package com.atguigu.spark.chapter09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/3/20
  * Desc:  transform算子
  *   -无状态转换算子
  *   -扩展SparkStreamingAPI
  */
object Streaming03_Nostate_Transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")
    //创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从指定的网络端口读取数据，创建DS
    val lineDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202",9999)

    /*
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)
    */
    //转换为RDD
    val resDS: DStream[(String, Int)] = lineDS.transform(
      rdd => {
        val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        reduceRDD.sortByKey()
      }
    )
    resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
