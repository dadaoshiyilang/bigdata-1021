package com.atguigu.spark.chapter10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object scalaTest {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("scalaTest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new SparkContext(sparkConf)

    val rddA: RDD[(String, Int)] = ssc.makeRDD(Array(("a",1),("a",2),("b",1),("c",1)))

    val rddB: RDD[(String, Int)] = ssc.makeRDD(Array(("a",1),("b",1),("b",2),("d",1)))

    rddA.join(rddB).collect().foreach(println)

    println("---------------------------")
    rddA.leftOuterJoin(rddB).collect().foreach(println)
    println("---------------------------")
    rddA.rightOuterJoin(rddB).collect().foreach(println)
    println("---------------------------")
    rddA.fullOuterJoin(rddB).collect().foreach(println)

  }

}
