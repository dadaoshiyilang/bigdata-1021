package com.atguigu.spark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_TopN {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN")

    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input\\agent.log")

    val mpRdd: RDD[(String, Int)] = fileRdd.map(log => {
      val datas: Array[String] = log.split(" ")
      (datas(1) + "-" + datas(4), 1)
    })

    val reduRdd: RDD[(String, Int)] = mpRdd.reduceByKey((x, y) => (x + y))
    val splitRdd: RDD[(String, (String, Int))] = reduRdd.map {
      case (words, cnt) => {
        val spls: Array[String] = words.split("-")
        (spls(0), (spls(1), cnt))
      }
    }

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = splitRdd.groupByKey()

    val mapValuesRdd = groupRdd.mapValues {
      datas => {
        datas.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    }

    mapValuesRdd.collect().foreach(println)
    sc.stop()
  }
}
