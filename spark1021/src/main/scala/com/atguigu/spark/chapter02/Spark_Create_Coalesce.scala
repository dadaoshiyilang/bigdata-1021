package com.atguigu.spark.chapter02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_Create_Coalesce {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Coalesce")

    val sc: SparkContext = new SparkContext(conf)

    val scRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1,3,4), 5)

    scRdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"=======before========"+datas.mkString(","))
        datas
      }
    ).collect()

//    val coalesceRdd: RDD[Int] = scRdd.coalesce(3)

    val repRdd: RDD[Int] = scRdd.repartition(6)

    repRdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"=======after========"+datas.mkString(","))
        datas
      }
    ).collect()

    sc.stop()
  }
}