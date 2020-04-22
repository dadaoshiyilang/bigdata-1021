package com.atguigu.spark.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_WordCount02 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_WordCount02")

    val sc: SparkContext = new SparkContext(conf)

    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")

    val rdd: RDD[String] = sc.makeRDD(strList)

    //扁平化
    val mapRdd: RDD[String] = rdd.flatMap(_.split(" "))

    val groupByRdd: RDD[(String, Iterable[String])] = mapRdd.groupBy(key=>key)

    val sumRdd: RDD[(String, Int)] = groupByRdd.map {
      case (words, datas) => (words, datas.size)
    }

    val sumRdd01: RDD[(String, Int)] = groupByRdd.map(m => (m._1, m._2.size))

    sumRdd01.collect().foreach(println)

    sc.stop()

}

}
