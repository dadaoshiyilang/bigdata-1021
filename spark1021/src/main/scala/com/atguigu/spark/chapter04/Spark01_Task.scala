package com.atguigu.spark.chapter04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/3/13
  * Desc: 任务划分
  * 跟踪源码：
  *   >应用Application
  *   >Job
  *     行动算子被触发的次数
  *   >Stage
  *     当前Job中宽依赖的数量 + 1
  *   >Task
  *     当前阶段最后一个RDD的分区数
  */
object Spark01_Task {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Spark01_Task").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    val resRDD: RDD[(Int, Int)] = dataRDD.map((_,1)).reduceByKey(_+_)

    //将数据打印到控制台
    resRDD.collect().foreach(println)
    //将数据保存到文件
    resRDD.saveAsTextFile("D:\\workspace\\bigdata-1021\\spark1021\\output")

    Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
