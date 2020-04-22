package com.atguigu.scala.chapter07

object Scala_Test_Map_flatten {

  def main(args: Array[String]): Unit = {

    val list: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val list01: List[Int] = list.map(x => {x + 1})

    println(list01)

    val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    // 多个list归并到一个list中 List(1,2,3,4,5,6,7,8,9)
    println(nestedList.flatten)

    val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")
    val listArrays: List[Array[String]] = wordList.map(s => {s.split(" ")})

    println(listArrays.flatten)

    val listFlatMap: List[String] = wordList.flatMap(s => {s.split(" ")})

    println(listFlatMap)

  }

}
