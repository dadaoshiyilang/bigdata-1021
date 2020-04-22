package com.atguigu.scala.chapter06

object Scala_Test_MulArray {
  def main(args: Array[String]): Unit = {

    // 创建二位数组
    val array: Array[Array[Int]] = Array.ofDim(2, 3)
    array(1)(0)= 10

    for (i <- 0 until  array.length; j <- 0 until array(i).length) {
      println(array(i)(j))
    }

    // 创建多维数组
    val array1: Array[Array[Array[Array[Array[Int]]]]] = Array.ofDim(2,3,4,5,6)

  }

}
