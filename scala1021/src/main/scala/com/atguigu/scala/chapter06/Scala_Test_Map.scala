package com.atguigu.scala.chapter06

import scala.collection.mutable

object Scala_Test_Map {

  def main(args: Array[String]): Unit = {

    // 创建不可变的Map
    val map01: Map[String, Int] = Map("a"->1,"b"->2,"c"->3)

    // 遍历map
    for (k <- map01.keys) {
      println(k + "---"+map01.get(k))// option 类型返回，没有返回none,有的话返回Some
    }

    println(map01.get("a").get)
    println(map01.getOrElse("d", 0))

    // 可变的map
    val map02: mutable.Map[String, Int] = mutable.Map("a"->1,"b"->2,"c"->3)

    map02.put("d", 10)

    map02.remove("a")


    map02.update("b", 20)
    map02("c")=30
    map02.foreach(println)



  }

}
