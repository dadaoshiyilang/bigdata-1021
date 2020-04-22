package com.atguigu.scala.chapter06

import scala.collection.mutable.ListBuffer

object Scala_Test_ListBuffer {

  def main(args: Array[String]): Unit = {

    // 创建一个可变集合
     val list01: ListBuffer[Int] = ListBuffer(1,2,3)

    list01.+=(10)
    list01.insert(1,19)
    list01.append(29)

    // 修改集合元素
    list01(0) = 90
    list01.update(3, 89)

    // 删除list集合元素
    list01.remove(0)

    // 会创建一个新的集合
    val list02: ListBuffer[Int] = list01.-(2)

    // 不会创建一个新的集合，list01和list03都会把该元素删掉
    val list03: ListBuffer[Int] = list01.-=(2)

    println(list01)
    println(list02)
    println(list03)

  }

}
