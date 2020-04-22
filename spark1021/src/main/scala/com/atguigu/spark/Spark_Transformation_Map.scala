package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Transformation_Map {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Transformation_Map")

    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8), 2)

//    val mapRdd: RDD[Int] = rdd.map(_*2)

//    val mapRdd = rdd.mapPartitions(elem=>elem.map(_*2))

//      val mapRdd = rdd.mapPartitionsWithIndex((index, datas) => datas.map(dt =>(index, dt)))

    val mapRdd = rdd.mapPartitionsWithIndex((index, datas) => {
      index match {
        case 1 => datas.map(_ * 2)
        case _ => datas
      }
    })


    mapRdd.collect().foreach(println)

    Thread.sleep(10000000)

        sc.stop()
  }

}
