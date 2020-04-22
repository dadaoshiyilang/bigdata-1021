package com.atguigu.spark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/3/14
  * Desc: 自定义累加器
  *   需求：自定义累加器，统计集合中首字母为“H”单词出现的次数。
  */
object Spark02_Accumulator {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hive", "Hive", "Hello", "Spark", "Spark"))

    //创建一个自定义的累加器对象
    val myAC = new MyAccumulator

    //注册累加器到sc
    sc.register(myAC)

    //使用累加器
    rdd.foreach{
      word=>{
        //将单词放入到累加器中
        myAC.add(word)
      }
    }

    //打印累加器输出结果
    println(myAC.value)

    // 关闭连接
    sc.stop()
  }
}
//泛型中需要指定累加器接收的输入类型，以及输出类型
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
  //定义一个空的map集合，用于存放累加器累加内容
  var map = mutable.Map[String,Int]()

  //判断是否为初始化状态
  override def isZero: Boolean = map.isEmpty

  //拷贝
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newAcc = new MyAccumulator
    newAcc.map = this.map
    newAcc
  }

  //重置
  override def reset(): Unit = map.clear()

  //向累加器中加入数据
  override def add(inputStr: String): Unit = {
    if(inputStr.startsWith("H")){
      map(inputStr) = map.getOrElse(inputStr,0) + 1
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    //当前Executor的map
    var map1 = this.map
    //另一个需要合并的累加器中的map集合
    var map2 = other.value
    //对两个map集合进行合并
    map = map1.foldLeft(map2){
      (mmpp,kv)=>{
        val word: String = kv._1
        val count: Int = kv._2
        mmpp(word) = mmpp.getOrElse(word,0) + count
        mmpp
      }
    }
  }

  //获取累加器的值
  override def value: mutable.Map[String, Int] = map
}