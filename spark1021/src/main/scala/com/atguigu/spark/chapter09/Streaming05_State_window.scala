package com.atguigu.spark.chapter09

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
  * Author: Felix
  * Date: 2020/3/18
  * Desc:  有状态的转换算子-window
  *   -窗口大小（时长）
  *   -窗口滑动距离
  *   -以上两者都必须满足是采集周期整数倍
  */
object Streaming05_State_window {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    //注意：因为采集器会开启单独的线程，所以这里local后面必须执行CPU核数
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming01_WordCount")

    //设置优雅的关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //创建SparkStreaming上下文环境对象  第二个参数，采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点  用于保存状态
    ssc.checkpoint("D:\\dev\\workspace\\bigdata-1021\\spark-1021\\cp")

    //从指定的端口获取数据，创建DS
    val lineDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202",9999)

    //扁平映射
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    val windowDS: DStream[String] = flatMapDS.window(Seconds(6),Seconds(3))

    //对窗口中的数据进行结构的转换
    val mapDS: DStream[(String, Int)] = windowDS.map((_,1))

    //聚合
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //resDS.print()

    //使用累加器和广播变量
    //ssc.sparkContext.longAccumulator
    //ssc.sparkContext.broadcast()
    //使用缓存和检查点
    //ssc.checkpoint()
    //resDS.cache()
    //resDS.persist()

    //保存数据到文件上
    //resDS.saveAsTextFiles("D:\\dev\\workspace\\bigdata-1021\\spark-1021\\output\\test")


    //使用SparkSQL处理采集周期中的数据
    //创建SparkSession执行入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //DS变成RDD
    resDS.foreachRDD(
      rdd=>{
        //转成df
        val df: DataFrame = rdd.toDF("word","count")
        //创建一个临时视图
        df.createOrReplaceTempView("words")
        //执行SQL进行查询
        spark.sql("select * from words").show
      }
    )

    //启动采集器
    ssc.start()

    // 启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {
        while ( true ) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex : Exception => println(ex)
          }

          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop202:9000"), new Configuration(), "atguigu")

          val state: StreamingContextState = ssc.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if ( state == StreamingContextState.ACTIVE ) {

            // 判断路径是否存在
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop202:9000/stopSpark"))
            if ( flg ) {
              ssc.stop(true, true)
              System.exit(0)
            }

          }
        }

      }
    }).start()

    //等待采集结束之后，终止上下文环境对象
    ssc.awaitTermination()
  }
}
