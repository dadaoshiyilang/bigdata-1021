package com.atguigu.spark.chapter04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

object Spark_Create_Json {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Mem")

    val sc: SparkContext = new SparkContext(conf)

    val josonRdd: RDD[String] = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input\\3.json")

    val jsRdd: RDD[Option[Any]] = josonRdd.map(JSON.parseFull)

    jsRdd.collect().foreach(println)

    sc.stop()

  }

}
