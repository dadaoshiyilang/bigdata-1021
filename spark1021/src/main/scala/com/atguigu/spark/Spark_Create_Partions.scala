package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Partions {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark_Create_Partions")

    val sc: SparkContext = new SparkContext(conf)

    // val lists: List[Int] = List(1,2,3,4)

//    val rdd: RDD[Int] = sc.makeRDD(lists)

//    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)

    val rdd = sc.textFile("D:\\workspace\\bigdata-1021\\spark1021\\input\\test.txt",3)

    rdd.saveAsTextFile("D:\\workspace\\bigdata-1021\\spark1021\\output")



         sc.stop()

  }

}
