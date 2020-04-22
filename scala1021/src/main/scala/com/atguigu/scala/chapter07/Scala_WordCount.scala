package com.atguigu.scala.chapter07

object Scala_WordCount {

  def main(args: Array[String]): Unit = {

    val lists: List[(String, Int)] = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    val flatMapList: List[(String, Int)] = lists.flatMap(t => {
      val line: String = t._1
      val words = line.split(" ")
      words.map(w => (w, t._2))
    })

    // hello -> List((hello, 4), (hello, 3))
    val groupWordMap: Map[String, List[(String, Int)]] = flatMapList.groupBy(t =>t._1)

//    groupWordMap.map(t => {
//      val countlist: List[Int] = t._2.map(tt => tt._2)
//      (t._1, countlist)
//    })

    //
    val wordSums: Map[String, Int] = groupWordMap.mapValues(datas =>datas.map(tt =>tt._2).sum)

    val sortList: List[(String, Int)] = wordSums.toList.sortWith((left, right) => left._2 > right._2)

    val resList: List[(String, Int)] = sortList.take(3)

    println(resList)

    val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")

    // List(hello, world, hello, atguigu, hello, scala)
    println(wordList.flatMap(x => x.split(" ")))

    // 2) 将相同的单词放置在一起
    val wordToWordsMap: Map[String, List[String]] = wordList.groupBy(word=>word)
    // Map(hello atguigu -> List(hello atguigu), hello scala -> List(hello scala), hello world -> List(hello world))
    println(wordToWordsMap)

    // 3) 对相同的单词进行计数
    // (word, list) => (word, count)
    val wordToCountMap: Map[String, Int] = wordToWordsMap.map(tuple=>(tuple._1, tuple._2.size))
    // Map(hello atguigu -> 1, hello scala -> 1, hello world -> 1)
    println(wordToCountMap)

    // 4) 对计数完成后的结果进行排序（降序）
    val sortList1: List[(String, Int)] = wordToCountMap.toList.sortWith {
      (left, right) => {
        left._2 > right._2
      }
    }




  }

}
