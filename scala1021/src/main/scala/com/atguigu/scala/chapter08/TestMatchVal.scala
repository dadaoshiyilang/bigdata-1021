package com.atguigu.scala.chapter08

object TestMatchVal {

  def main(args: Array[String]): Unit = {

    def abs(x: Int) = x match {
      case i: Int if i >= 0 => i
      case j: Int if j < 0 => -j
      case _ => "type illegal"
    }

    println(abs(-5))

  }

  def describe(x: Any) = x match {

    case 5 => "Int five"

    case "hello" => "String hello"

    case true => "Boolean true"

    case '+' => "Char +"

  }

}
