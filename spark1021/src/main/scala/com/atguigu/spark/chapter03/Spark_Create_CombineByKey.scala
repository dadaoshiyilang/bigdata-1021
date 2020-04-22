package com.atguigu.spark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_CombineByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_CombineByKey")

    val sc: SparkContext = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)

    sc.stop()
  }
}
