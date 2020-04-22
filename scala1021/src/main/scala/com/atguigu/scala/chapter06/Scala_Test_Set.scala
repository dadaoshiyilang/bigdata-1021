package com.atguigu.scala.chapter06

import scala.collection.mutable

object Scala_Test_Set {
  def main(args: Array[String]): Unit = {

    // 无序，不可重复
    val set01: Set[Int] = Set(1,3,5,6,7,9,5,3)

    // 向set集合中添加元素
    val set02: Set[Int] = set01.+(10)

    println(set01)

    println(set02)


    // 可变set
    val set03: mutable.Set[Int] = mutable.Set(1,2,3,7,4,5)

    // 向set集合中添加元素
    set03.add(10)

    //创建一个新的集合，set03不会添加20
    val set04: mutable.Set[Int] = set03.+(20)

    // 删除元素
    val bool: Boolean = set03.remove(2)

    set03.drop(2)
    println(set03)
    println(set04)

  }

}
