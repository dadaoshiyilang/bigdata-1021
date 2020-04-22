package com.atguigu.spark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object Spark_Create_Accumulator {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Mem")

    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hadoop", "Hive", "HBase", "Hello", "Spark", "Spark"))

    val myAC = new MyAccumulator1

    //注册累加器到sc
    sc.register(myAC)

    rdd.foreach{
      rd => {
        myAC.add(rd)
      }
    }

    //打印累加器输出结果
    println(myAC.value)

    sc.stop()

  }
}

class MyAccumulator1 extends AccumulatorV2[String,mutable.Map[String,Int]]{

  var map = mutable.Map[String,Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val accumulator = new MyAccumulator
    accumulator.map = this.map
    accumulator
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if (v.startsWith("H")) {
      map(v) = map.getOrElse(v, 0) + 1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1: mutable.Map[String, Int] = this.map
    val map2: mutable.Map[String, Int] = other.value

    map1.foldLeft(map2){
      (mp2, kv) => {
        val key: String = kv._1
        val values: Int = kv._2
        mp2(key) = mp2.getOrElse(key, 0) + values
        mp2
      }
    }
  }

  override def value: mutable.Map[String, Int] = map
}