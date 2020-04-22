package com.atguigu.scala.chapter09

object TestImplicitParameter {

  implicit val str:String = "hello world"

  def hello(implicit arg:String="good bey world!") = {
    println(arg)
  }

  def main(args: Array[String]): Unit = {

    hello
  }

}
