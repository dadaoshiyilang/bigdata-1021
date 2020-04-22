package com.atguigu.spark.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_WordCount01 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_WordCount01")

    val sc: SparkContext = new SparkContext(conf)

    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")

    val rdd: RDD[String] = sc.makeRDD(strList)

    //扁平化
    val mapRdd: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRdd01: RDD[(String, Int)] = mapRdd.map(mp =>(mp, 1))

    /**
      * (Spark,CompactBuffer((Spark,1)))
      * (Hello,CompactBuffer((Hello,1), (Hello,1), (Hello,1)))
      * (World,CompactBuffer((World,1)))
      * (Scala,CompactBuffer((Scala,1)))
      */
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd01.groupBy(t =>t._1)

    // 匹配一模式
    val sumRdd: RDD[(String, Int)] = groupRdd.map(mp=>(mp._1, mp._2.size))

    // 元组匹配模式
    val sumRdd01: RDD[(String, Int)] = groupRdd.map {
      case (words, datas) => (words, datas.size)
    }

    sumRdd01.collect().foreach(println)

    sc.stop()

  }

}
