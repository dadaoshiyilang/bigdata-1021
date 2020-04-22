package com.atguigu.spark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_FoldByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_FoldByKey")

    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    //3.2 取出每个分区相同key对应值的最大值，然后相加
    //rdd.aggregateByKey(0)(math.max(_, _), _ + _).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop()
  }
}
