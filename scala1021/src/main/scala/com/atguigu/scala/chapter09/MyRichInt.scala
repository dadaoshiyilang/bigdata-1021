package com.atguigu.scala.chapter09

class MyRichInt(val self: Int) {
  def myMax(i: Int): Int = {
    if (self < i) i else self
  }

  def myMin(i: Int): Int = {
    if (self < i) self else i
  }
}

object MyRichInt {

  implicit def convert(arg: Int): MyRichInt = {
    new MyRichInt(arg)
  }
  def main(args: Array[String]): Unit = {

    println(2.myMax(6))

  }

}
