package com.atguigu.spark.chapter08

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream_WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkStream_WordCount").setMaster("local[*]")

    // Duration
    val scc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 离散化流
    val scoketDS: ReceiverInputDStream[String] = scc.socketTextStream("hadoop102", 9999)

    val flatMapDS = scoketDS.flatMap(_.split(" "))

    val mapDs: DStream[(String, Int)] = flatMapDS.map((_, 1))

    val reduceDs: DStream[(String, Int)] = mapDs.reduceByKey(_+_)

    reduceDs.print()

    // 启动采集器
    scc.start()

    // 等待采集结果，终止上下文环境对象
    scc.awaitTermination()

  }

}
