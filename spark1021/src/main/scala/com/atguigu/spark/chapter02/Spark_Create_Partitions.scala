package com.atguigu.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark_Create_Partitions {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Partitions")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    rdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"=======before========"+datas.mkString(","))
        datas
      }
    ).collect()


//    val parRdd: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))

    val parRdd: RDD[(Int, String)] = rdd.partitionBy(new myPartitions(2))

    parRdd.mapPartitionsWithIndex(
      (index, datas) => {
        println(index +"=======after========"+datas.mkString(","))
        datas
      }
    ).collect()

    parRdd.collect().foreach(println)

    sc.stop()
  }

  class  myPartitions(partitions: Int) extends  Partitioner {

    override  def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      if(key.toString.startsWith("134")){
        0
      }else if(key.toString.startsWith("135")){
        1
      }else{
        2
      }
    }
  }

}