package com.atguigu.scala.chapter08

object TestMatchList {

  def main(args: Array[String]): Unit = {

    val list: List[Int] = List(1, 2, 5, 6, 7)

    list match {
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }
  }

}
