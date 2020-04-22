package com.atguigu.scala.chapter08

object Scala_Test_List01 {

  def main(args: Array[String]): Unit = {

    val list01: List[Int] = List(1,2,3,4,5,6,7,8,9)

    println("对集合进行filter过滤："+list01.filter(x=> x%2==0))

    println("map转换映射："+list01.map(x => x*2))// List(2, 4, 6, 8, 10, 12, 14, 16, 18)

    val list03: List[List[Int]] = List(List(1,2,3),List(4,5,6),List(7,8,9))
    println("flatten扁平化："+list03.flatten) //

    val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")

    println("map:"+wordList.map(s => s.split(" ")))

    println("flatten:"+ wordList.map(s => s.split(" ")).flatten)

    println("扁平化+map=flatMap："+ wordList.flatMap(x=>x.split(" ")))

    println("分组："+list01.groupBy(x => x % 2))





  }

}
