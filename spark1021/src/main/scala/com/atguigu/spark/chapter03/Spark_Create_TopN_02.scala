package com.atguigu.spark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_TopN_02 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN_02")

    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input\\agent.log")

    //3.按照最小粒度聚合：((Province,AD),1)
    val provinceAdToOne = fileRdd.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }

    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum) ((2,8),13)
    val provinceAdToSum = provinceAdToOne.reduceByKey(_ + _)

    //5.将省份作为key，广告加点击数为value：(Province,(AD,sum))
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))

    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup = provinceToAdSum.groupByKey()

    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }

    //8.将数据拉取到Driver端并打印
    provinceAdTop3.collect().foreach(println)

    sc.stop()
  }
}