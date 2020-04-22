package com.atguigu.scala.chapter08

object TestList {

  def main(args: Array[String]): Unit = {

    val list01: List[Int] = List(1,2,3,4,5,6,7,8,9)
    // 获取list的长度
    println(list01.length)
    // 获取list的大小
    println(list01.size)
    // 循环遍历
    list01.foreach(println)
    // 迭代器
    for (elem <- list01.iterator) {
      println(elem)
    }

    // 生成字符串
    println(list01.mkString(","))

    // 是否包含
    println(list01.contains(9))

    // 衍生集合
    // 获取集合的头
    println("获取集合的头："+list01.head)

    // 获取集合的尾
    println("获取集合的尾："+list01.tail)

    println("获取集合中的最后一个元素："+list01.last)

    println("获取集合的初始数据，不包含最后一个："+list01.init)

    println("反转集合："+list01.reverse)

    println("获取集合前n个元素："+list01.take(3))

    println("获取集合后n个元素："+list01.takeRight(3))

    println("去掉集合前n个元素："+list01.drop(3))
    println("去掉集合后n个元素："+list01.dropRight(3))

    val list02: List[Int] = List(3,4,5,6,7,10,11)

    println("获取集合的并集："+list01.union(list02))

    println("获取集合的交集："+list01.intersect(list02))

    println("获取集合的差集："+list01.diff(list02))

    println("获取集合的拉链："+list01.zip(list02))

    val list3: List[Int] = List(1, 2, 3, 4, 5, 6, 7)

    list3.sliding(2, 5).foreach(println)







  }

}
