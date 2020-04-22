package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Sample {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Sample")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    // true:表示抽取放回
//    val samRdd: RDD[Int] = rdd.sample(true, 1)

    val samRdd: RDD[Int] = rdd.sample(false, 0.6)

    samRdd.collect().foreach(println)

    sc.stop()

  }

}
