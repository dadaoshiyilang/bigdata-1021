package com.atguigu.scala.chapter09

object TestImplicitClass {

  // 隐式类
 implicit class MyRichInt(arg:Int) {

    def myMax(i:Int):Int = {
      if (arg < i) i else arg
    }

    def  myMin(i:Int):Int = {
      if (arg < i) arg else i
    }

  }

  def main(args: Array[String]): Unit = {
    println(2.myMax(7))

  }

}
