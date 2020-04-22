package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)

//    val lineRdd: RDD[String] = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input")
//
//    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))
//
//    val wds: RDD[(String, Int)] = wordRdd.map(m => (m, 1))
//
//    val wdSum: RDD[(String, Int)] = wds.reduceByKey((v1, v2) =>(v1 + v2))
//    val wdCnt: Array[(String, Int)] = wdSum.collect()
//
//    wdCnt.foreach(println)
    sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ +_).saveAsTextFile("output")
    sc.stop()
  }
}
