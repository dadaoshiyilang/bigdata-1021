package com.atguigu.spark.chapter06

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL01_Demo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("D:\\workspace\\bigdata-1021\\spark1021\\input\\test.json")

//    df.show()

    // sql风格语法
    df.createOrReplaceTempView("user")
    // 平均年龄
    // spark.sql(" select avg(age) from user").show()

    // DSL风格语法
//    df.select("name","age").show()

    // RDD => DataFrame转换
    // RDD
    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "qiaofeng", 30),(2, "xuzhu", 28), (3, "qiaofeng", 20)))

    // Rdd转dataFrame
    val df2: DataFrame = rdd1.toDF()
//    df2.show()


    val rdd2: RDD[Row] = df2.rdd

    // RDD 返回的Rdd类型为row,里面提供的getxxx方法可以获取字段值，索引从0开始
    // rdd2.foreach(a => println(a.getString(1)))


    // RDD=> DataSet**

    val rddDs: RDD[User] = rdd1.map {
      case (id, name, age) => User(id, name, age)
    }
    rddDs.toDS().show()







  }

}

case class User(i: Int, str: String, i1: Int)
