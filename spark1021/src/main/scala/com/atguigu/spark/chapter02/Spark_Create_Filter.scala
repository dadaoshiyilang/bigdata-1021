package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Filter {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Filter")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8), 2)

    val filRdd: RDD[Int] = rdd.filter(_%2 == 0)

    filRdd.collect().foreach(println)

    sc.stop()

  }

}
