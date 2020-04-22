package com.atguigu.spark.chapter06

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark04_TestAgeDsAvg {

  def main(args: Array[String]): Unit = {
    //创建配置对象
    //val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL05_AVG_UDAF1")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Spark04_TestAgeDsAvg")
      .getOrCreate()

    import spark.implicits._

    //读取json文件，创建DF
    val df: DataFrame = spark.read.json("D:\\workspace\\bigdata-1021\\spark1021\\input\\test.json")

    //将DF转换为DS
    val ds: Dataset[User001] = df.as[User001]

    //创建自定义函数对象
    val myAvg = new MyAvgUDAF

    //将自定义函数对象转换为列(将函数应用于查询当中)
    val col: TypedColumn[User001, Double] = myAvg.toColumn

    //使用DSL风格对DS数据进行查询
    ds.select(col).show

    //释放资源
    spark.stop()
  }
}


//输入数据类型
case class User001(name:String,age:Long){}

//缓存类型
case class AgeBuffer(var sum:Long,var count:Long) {}

//自定义UDAF函数 强类型
//  IN  输入数据类型
//  BUF 缓存数据类型
//  OUT 输出数据类型
class MyAvgUDAF extends Aggregator[User001, AgeBuffer, Double]{

  //缓存初始化
  override def zero: AgeBuffer = {
    AgeBuffer(0L,0L)
  }

  //聚合
  override def reduce(b: AgeBuffer, a: User001): AgeBuffer = {
    b.sum += a.age
    b.count += 1
    b
  }

  //合并
  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //求值
  override def finish(reduction: AgeBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }

  //指定DataSet的编码方式  用于进行序列化   固定写法
  //如果是简单类型，就使用系统自带的；如果说，是自定义类型的话，那么就使用product
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}