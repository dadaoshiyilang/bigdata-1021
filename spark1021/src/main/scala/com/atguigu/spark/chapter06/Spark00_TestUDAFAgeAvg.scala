package com.atguigu.spark.chapter06

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Spark00_TestUDAFAgeAvg {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("local[*]").setAppName("Spark00_TestUDAFAgeAvg")).getOrCreate()

    import spark.implicits._

    val myDAF: MyAveragUDAF = new MyAveragUDAF

    spark.udf.register("avgAge", myDAF)


    val df: DataFrame = spark.read.json("D:\\workspace\\bigdata-1021\\spark1021\\input\\test.json")

    // 创建临时视图
    df.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) from user").show()




  }

}

class MyAveragUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = {
    StructType(Array(StructField("age", IntegerType)))
  }

  override def bufferSchema: StructType = {
    StructType(Array(StructField("sum", LongType),StructField("cnt", LongType)))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true // 稳定性判断 =>输入数据相同，输出数据是否一样

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 设置年龄的总和和人数为初始值0
    buffer(0) = 0L
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 判断是否为空
//    if (buffer.isNullAt(0))
    buffer(0) = buffer.getLong(0) + input.getInt(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble/buffer.getLong(1)

}
