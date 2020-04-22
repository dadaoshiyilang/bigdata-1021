package com.atguigu.spark.chapter08

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream_Receiver {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkStream_Receiver").setMaster("local[*]")

    // Duration
    val scc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 自定义数据源
    scc.receiverStream(new MyReceiver)

    // 启动采集器
    scc.start()

    // 等待采集结果，终止上下文环境对象
    scc.awaitTermination()

  }

}

class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

  override def onStart(): Unit = {

  }

  override def onStop(): Unit = {

  }
}
