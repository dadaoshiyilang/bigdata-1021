package com.atguigu.scala.chapter06

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Scala_Test_ArrayBuffer {

  def main(args: Array[String]): Unit = {

    // 不可变数组和可变数组之间的转换
    // 可变数组
    val arry01: ArrayBuffer[Int] = ArrayBuffer(1,3,4)

    // 可变数组转换为不可变数组
    val array: Array[Int] = arry01.toArray

    val buffer: mutable.Buffer[Int] = array.toBuffer



  }

}
