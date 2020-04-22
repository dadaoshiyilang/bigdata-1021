package com.atguigu.spark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_TopN_01 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN_01")

    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input\\agent.log")


    val mpRdd = fileRdd.map(log => {
      val logs: Array[String] = log.split(" ")
      (logs(1) + "-" + logs(4), 1)
    })

    val reduRdd: RDD[(String, Int)] = mpRdd.reduceByKey(_ + _)

    val mpRdd01: RDD[(String, (String, Int))] = reduRdd.map {
      case (words, cnt) => {
        val strs: Array[String] = words.split("-")
        (strs(0), (strs(1), cnt))
      }
    }

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mpRdd01.groupByKey()


    val takeRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(mp => {
      mp.toList.sortWith((x, y) => {
        x._2 > y._2
      }).take(3)
    })

    // 8,List((2,27), (20,23), (11,22)
    takeRdd.collect().foreach(println)

    sc.stop()
  }
}