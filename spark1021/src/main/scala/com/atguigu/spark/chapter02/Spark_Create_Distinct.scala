package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Distinct {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Distinct")

    val sc: SparkContext = new SparkContext(conf)

    val scRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1,3,4), 5)

    scRdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"==============="+datas.mkString(","))
        datas
      }
    ).collect()

    val disRdd: RDD[Int] = scRdd.distinct()

    disRdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"==============="+datas.mkString(","))
        datas
      }
    ).collect()

    sc.stop()



  }


}
