package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Mem {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Mem")

    val context: SparkContext = new SparkContext(conf)

    val list: List[Int] = List(1,2,3,4)

//    val rdd: RDD[Int] = context.parallelize(list)

    val rdd: RDD[Int] = context.makeRDD(list)

    println("分区数："+rdd.partitions.size)

    rdd.collect().foreach(println)


  }

}
