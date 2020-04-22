package com.atguigu.scala.chapter07

    object ScalaTestWordCount {

      def main(args: Array[String]): Unit = {

        val tuples = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

        // (Hello,4),(Scala,4),(Spark,4),(World,4)
        // (Hello,3),(Scala,3),(Spark,3)
        // (Hello,2),(Scala,2)
        // (Hello,1)
        val wordToCountList: List[(String, Int)] = tuples.flatMap {
          t => {
            val strings: Array[String] = t._1.split(" ")
            strings.map(word => (word, t._2))
          }
        }

        // Hello, List((Hello,4), (Hello,3), (Hello,2), (Hello,1))
        // Scala, List((Scala,4), (Scala,3), (Scala,2)
        // Spark, List((Spark,4), (Spark,3)
        // Word, List((Word,4))
        val wordToTupleMap: Map[String, List[(String, Int)]] = wordToCountList.groupBy(t=>t._1)



        val stringToInts: Map[String, Int] = wordToTupleMap.mapValues(
          datas => datas.map(t => t._2).sum
        )
        println(stringToInts)

        /*
        val wordToCountMap: Map[String, List[Int]] = wordToTupleMap.map {
            t => {
                (t._1, t._2.map(t1 => t1._2))
            }
        }

        val wordToTotalCountMap: Map[String, Int] = wordToCountMap.map(t=>(t._1, t._2.sum))
        println(wordToTotalCountMap)
        */

      }

}
