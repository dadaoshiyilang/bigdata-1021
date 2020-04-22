package com.atguigu.spark.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark00_TestAgeAvg {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Mem")

    val sc: SparkContext = new SparkContext(conf)

    val mkRdd: RDD[(String, Int)] = sc.makeRDD(List(("zs", 20), ("lisi", 30), ("wangw", 40)))

    val mapRdd: RDD[(Int, Int)] = mkRdd.map {
      case (name, age) => {
        (age, 1)
      }
    }
    val reduRdd: (Int, Int) = mapRdd.reduce {
      case (x, y) => {
        (x._1 + y._1, x._2 + y._2)
      }
    }

    println(reduRdd._1/reduRdd._2)




    sc.stop()

  }

}