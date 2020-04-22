package com.atguigu.scala.chapter08

object Scala_Test_WordCountSecond {

  def main(args: Array[String]): Unit = {
    val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    val list01: List[(String, Int)] = tupleList.flatMap {
      t => {
        val strings: Array[String] = t._1.split(" ")
        strings.map(word => (word, t._2))
      }
    }

    println(list01)

    val wordToTupleMap: Map[String, List[(String, Int)]] = list01.groupBy(t=>t._1)

    // Map(Hello -> List((Hello,4), (Hello,3), (Hello,2), (Hello,1)), Spark -> List((Spark,4), (Spark,3)), Scala -> List((Scala,4), (Scala,3), (Scala,2)), World -> List((World,4)))
    println(wordToTupleMap)

    val stringToInts: Map[String, Int] = wordToTupleMap.mapValues(
      datas => datas.map(t => t._2).sum)

    val tuples = stringToInts.toList.sortWith((x,y)=> x._2 > y._2)

    println(tuples.take(3))

  }

}
