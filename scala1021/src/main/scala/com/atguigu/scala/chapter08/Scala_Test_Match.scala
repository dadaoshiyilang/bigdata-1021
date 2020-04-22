package com.atguigu.scala.chapter08

object Scala_Test_Match {

  def main(args: Array[String]): Unit = {


    var list: List[(String, Int)] = List(("a", 1), ("b", 2), ("c", 3))
    //println(list.map(t => (t._1, t._2 * 2)))
    println(
      list.map{
        case (word,count)=>(word,count*2)
      }
    )


    println(
      list.map(t => {
        (t._1, t._2*2)
      })
    )


    val tuples: List[(String, (String, Int))] = List( ("a", ("a", 1)), ("b", ("b", 2)), ("c", ("c", 3)) )

    println(
      tuples.map(t => {
        (t._1, (t._2._1,t._2._2*2))
      })
    )

    val list01: List[(String, (String, Int))] = tuples.map {
      case (groupkey, (word, count)) => (groupkey, (word, count*2))
    }
    println(list01)

  }

}
