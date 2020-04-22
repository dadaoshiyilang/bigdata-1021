package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Glom {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Glom")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 9, 3)

    rdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"==============="+datas.mkString(","))
        datas
      }
    ).collect()

    val glomRdd: RDD[Int] = rdd.glom().map(_.max)

    glomRdd.collect().foreach(println)

    sc.stop()


  }

}
