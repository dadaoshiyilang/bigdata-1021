package com.atguigu.scala.chapter06

import scala.collection.mutable.ArrayBuffer

object Scala02_TestArray {

  def main(args: Array[String]): Unit = {
    val buffer: ArrayBuffer[Nothing] = new ArrayBuffer()

    val arr: ArrayBuffer[Int] = ArrayBuffer(1,2,3)

    arr.append(5)

    val arr1: ArrayBuffer[Int] = arr.+=(20)

    println(arr.mkString(","))

    println(arr1.mkString(","))

    val arr2: ArrayBuffer[Int] = arr .+=:(40)


    val arr3=arr :+(20)//后加 arr1变了 arr没变
    val arr4=arr +=20//后加 arr1 和 arr都变了


    println(arr.mkString(","))
    println(arr2.mkString(","))

  }
}
