package com.atguigu.spark.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_Create_WordCount03 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_WordCount03")

    val sc: SparkContext = new SparkContext(conf)

    var strList: List[(String, Int)] = List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2))

    val rdd: RDD[(String, Int)] = sc.makeRDD(strList)

    val fltRdd: RDD[(String, Int)] = rdd.flatMap(fl => {
      val arrys: mutable.ArrayOps[String] = fl._1.split(" ")
      // 数组结构转换
      arrys.map(mp => (mp, fl._2))
    })

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = fltRdd.groupBy(key => key._1)


    val sumRdd = groupRdd.map {
      case (words, datas) => (words, datas.map(t => t._2).sum)
    }

    sumRdd.collect().foreach(println)


    sc.stop()

  }

}
