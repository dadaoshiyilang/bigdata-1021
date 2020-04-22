package com.atguigu.scala.chapter06

object Scala_Test_Tuple {

  def main(args: Array[String]): Unit = {

    // 创建元组
    //参数类型=>返回类型
    val tuple01: (String, String, Int) = ("100", "zs", 18)

    val tuple: List[(String, Int)] = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    // 访问元组
    //println(tuple01._1)
    //println(tuple01._2)
    //println(tuple01._3)

    // 通过索引访问元组元素
    //println(tuple01.productElement(1))

    // 通过迭代器访问元组元素
    for (el <- tuple.productIterator) {
      println(el)
    }

  }

}
