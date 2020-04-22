package com.atguigu.spark.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Spark00_TestAccumulatorAvg {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark00_TestAccumulatorAvg")

    val sc: SparkContext = new SparkContext(conf)

    val mkRdd: RDD[(String, Int)] = sc.makeRDD(List(("zs", 20), ("lisi", 30), ("wangw", 40)))

    val acmulator = new MyAcmulator

    sc.register(acmulator)
    mkRdd.foreach {
      case (name, age) => {
        acmulator.add(age)
      }
    }

    println(acmulator.value)

    sc.stop()

  }
}

class MyAcmulator extends AccumulatorV2[Int,Int] {

  var sum:Int = 0

  var cnt:Int = 0

  override def isZero: Boolean = {
    return (sum ==0 && cnt == 0)
  }

  override def copy(): AccumulatorV2[Int, Int] = {
    val acmulator = new MyAcmulator
    acmulator.sum = this.sum
    acmulator.cnt = this.cnt
    acmulator
  }

  override def reset(): Unit = {
    sum = 0
    cnt= 0
  }

  override def add(v: Int): Unit = {
    sum = sum + v
    cnt += 1
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {

    other match {
      case mc:MyAcmulator => {
        this.sum +=mc.sum
        this.cnt+=mc.cnt}
      case _=>
    }

  }

  override def value: Int = sum/cnt
}