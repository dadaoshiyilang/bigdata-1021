package com.atguigu.scala.chapter07

object TestWordCount {

  def main(args: Array[String]): Unit = {

    // 第一种方式（不通用）
    val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    val stringList: List[String] = tupleList.map(t=>(t._1 + " ") * t._2)

    //val words: List[String] = stringList.flatMap(s=>s.split(" "))
    val words: List[String] = stringList.flatMap(_.split(" "))

    //在map中，如果传进来什么就返回什么，不要用_省略
    val groupMap: Map[String, List[String]] = words.groupBy(word=>word)
    //val groupMap: Map[String, List[String]] = words.groupBy(_)

    // (word, list) => (word, count)
    val wordToCount: Map[String, Int] = groupMap.map(t=>(t._1, t._2.size))

    val wordCountList: List[(String, Int)] = wordToCount.toList.sortWith {
      (left, right) => {
        left._2 > right._2
      }
    }.take(3)

    //tupleList.map(t=>(t._1 + " ") * t._2).flatMap(_.split(" ")).groupBy(word=>word).map(t=>(t._1, t._2.size))
    println(wordCountList)
  }
}
