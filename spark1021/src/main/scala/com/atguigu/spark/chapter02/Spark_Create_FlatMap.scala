package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_FlatMap {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_FlatMap")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2), List(3,4),List(5,6),List(7)), 2)

    val flatRdd: RDD[Int] = rdd.flatMap(mp =>mp)

    flatRdd.collect().foreach(println)




    sc.stop()

  }

}
