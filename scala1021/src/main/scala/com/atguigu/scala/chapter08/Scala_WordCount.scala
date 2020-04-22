package com.atguigu.scala.chapter08

object Scala_WordCount {

  def main(args: Array[String]): Unit = {

    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")

    val list01: List[String] = stringList.flatMap(s => s.split(" "))

    // Map(Hello -> List(Hello, Hello, Hello, Hello), Hbase -> List(Hbase, Hbase),
    // kafka -> List(kafka), Scala -> List(Scala, Scala, Scala))
    val map01: Map[String, List[String]] = list01.groupBy(elem => elem)

    val map03: Map[String, Int] = map01.map(mp => (
      mp._1, mp._2.size
    ))
//    println(map03)
// Map(Hello -> 4, Hbase -> 2, kafka -> 1, Scala -> 3)

// List((Hello,4), (Scala,3), (Hbase,2))
    val resLists: List[(String, Int)] = map03.toList.sortWith((x, y) => x._2 >y._2)
    println(resLists.take(3))




  }

}
