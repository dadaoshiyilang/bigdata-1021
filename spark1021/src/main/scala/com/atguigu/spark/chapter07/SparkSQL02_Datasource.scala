package com.atguigu.spark.chapter07

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL02_Datasource {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

//    spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "t_test")
//      .load().show

//    spark.read.format("jdbc")
//      .options(Map("url"->"jdbc:mysql://localhost:3306/test?user=root&password=root",
//      "dbtable"->"t_test",
//      "driver"->"com.mysql.jdbc.Driver")).load().show

    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")

    spark.read.jdbc("jdbc:mysql://localhost:3306/test", "t_test", props).show()

    spark.stop()
  }
}
