package com.atguigu.spark.chapter04

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_JdbcRDD {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_JdbcRDD")

    val sc: SparkContext = new SparkContext(conf)

    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://localhost:3306/test"
    var username = "root"
    var passwd = "root"
    var sql:String = "select * from t_test where id >= ? and id <= ?"
    val resRdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, username, passwd)
      },
      sql,
      1,
      20,
      2,
      rs => {
        (rs.getInt(1), rs.getString(2), rs.getInt(3))
      }
    )
    resRdd.collect().foreach(println)
    sc.stop()
  }
}
