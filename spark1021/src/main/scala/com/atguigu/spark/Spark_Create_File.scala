package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_File {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_File")

    val sc: SparkContext = new SparkContext(conf)

    val rdds: RDD[String] = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input")

    rdds.collect().foreach(println)
    sc.stop()

  }

}
