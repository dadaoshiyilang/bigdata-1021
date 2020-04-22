package com.atguigu.spark.chapter07

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL01_HIve {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_HIve")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("show tables").show
  }
}