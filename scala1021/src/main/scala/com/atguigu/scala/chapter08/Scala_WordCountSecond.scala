package com.atguigu.scala.chapter08

import scala.collection.mutable

object Scala_WordCountSecond {

  def main(args: Array[String]): Unit = {

    val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    val tuples: List[(String, Int)] = tupleList.flatMap { mp => {
      val words: Array[String] = mp._1.split(" ")
      words.map(w => (w, mp._2))
      }
    }

    val map01: Map[String, List[(String, Int)]] = tuples.groupBy(k => k._1)

    val stringToInt: Map[String, Int] = map01.mapValues(datas => datas.map(t => t._2).sum)


        val wordToCountMap: Map[String, List[Int]] = map01.map {
            t => {
                (t._1, t._2.map(t1 => t1._2))
            }
        }

    println(">>>>>>>>"+wordToCountMap)
    /*
            val wordToTotalCountMap: Map[String, Int] = wordToCountMap.map(t=>(t._1, t._2.sum))
            println(wordToTotalCountMap)
            */



    val lists001: List[(String, Int)] = stringToInt.toList.sortWith((x, y)=>x._2>y._2)
    println(lists001.take(3))



  }

}
