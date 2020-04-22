package com.atguigu.spark.chapter08

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Author: Felix
  * Date: 2020/3/18
  * Desc:  需求：自定义数据源，实现监控某个端口号，获取该端口号内容
  */
object Streaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    //注意：因为采集器会开启单独的线程，所以这里local后面必须执行CPU核数
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Streaming01_WordCount")
    //创建SparkStreaming上下文环境对象  第二个参数，采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //从自定义的数据源获取数据，创建DS
    val lineDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver1("hadoop102",9999))

    //对获取到的一行数据进行扁平化处理
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    //结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //对数据进行聚合处理
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //输出结果
    reduceDS.print()
    //启动采集器
    ssc.start()
    //等待采集结束之后，终止上下文环境对象
    ssc.awaitTermination()
  }
}

class MyReceiver1(host: String,port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //定义一个Socket
  private var socket:Socket = _

  def receive() {
    try {
      socket = new Socket(host, port)
      //根据socket获取数据流
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
      var input:String = null
      //在生产环境中，如果要想终止读取的话，应该发送特定的标记   ==end==
      while((input=reader.readLine())!=null){
        store(input)
      }

    } catch {
      case e: ConnectException =>
        return
    } finally {
      onStop()
    }
  }

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }
}