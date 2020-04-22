package com.atguigu.spark.chapter09

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/3/18
  * Desc:  有状态的转换算子-updateStateByKey
  */
object Streaming04_State_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    //注意：因为采集器会开启单独的线程，所以这里local后面必须执行CPU核数
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming01_WordCount")
    //创建SparkStreaming上下文环境对象  第二个参数，采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点  用于保存状态
    ssc.checkpoint("D:\\dev\\workspace\\bigdata-1021\\spark-1021\\cp")

    //获取数据(离散化流)  -从指定服务器的指定端口获取
    val dataDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202",9999)

    //对获取到的一行数据进行扁平化处理
    val flatMapDS: DStream[String] = dataDS.flatMap(_.split(" "))

    //结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //对数据进行聚合处理
    //reduceByKey不会保存状态，只会对当前采集周期进行聚合
    //val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //Seq:当前采集周期内，每个key对应的value值
    //state:记录的是历史采集周期得到的数据结果
    val stateDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )

    //输出结果
    stateDS.print()

    //启动采集器
    ssc.start()
    //默认情况下，上下文对象不能关闭
    //ssc.stop()

    //等待采集结束之后，终止上下文环境对象
    ssc.awaitTermination()
  }
}
