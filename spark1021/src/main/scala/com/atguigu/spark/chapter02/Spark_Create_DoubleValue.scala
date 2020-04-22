package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_DoubleValue {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_DoubleValue")

    val sc: SparkContext = new SparkContext(conf)

    val scRdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val scRdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7))

    val resRdd: RDD[Int] = scRdd1.union(scRdd2)

    val scrdd: RDD[Int] = scRdd1.subtract(scRdd2)

    val scrdd01: RDD[Int] = scRdd1.intersection(scRdd2)

    val scrdd02: RDD[(Int, Int)] = scRdd1.zip(scRdd2)

    scrdd02.collect().foreach(println)

    sc.stop()
  }
}