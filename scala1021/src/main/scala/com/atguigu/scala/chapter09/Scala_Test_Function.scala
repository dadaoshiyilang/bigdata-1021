package com.atguigu.scala.chapter09

object Scala_Test_Function {

  def main(args: Array[String]): Unit = {

    val list = List(1,2,3,4,5,6,"test")

    val list01: List[Any] = list.map(m => {
      m match {
        case i: Int => i + 1
        case s: String => s
      }

    })
    println(list01.filter(x => x.isInstanceOf[Int]))

    println(list.filter(_.isInstanceOf[Int]).map(_.asInstanceOf[Int] + 1))

    println(list.collect{case x:Int => x +1})
  }

}
