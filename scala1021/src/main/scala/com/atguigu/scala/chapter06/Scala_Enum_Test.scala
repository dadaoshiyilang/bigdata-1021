package com.atguigu.scala.chapter06

object Scala_Enum_Test {
  def main(args: Array[String]): Unit = {
    println(Color.Red)

  }
}

object Color extends Enumeration {
  val Red = Value(1, "red")
  val yellow: Color.Value = Value(2, "yellow")
}

