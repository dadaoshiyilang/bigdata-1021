package com.atguigu.scala.chapter02

object Scala_Instance {
  def main(args: Array[String]): Unit = {
    Scala_Instance_Test.getInstance()
  }
}

class Scala_Instance_Test private {
  def getTestInstance()= println("Instance.....")
}

object Scala_Instance_Test {
  val instance = new Scala_Instance_Test
    def getInstance(): Scala_Instance_Test = {
      instance.getTestInstance()
      instance
    }
}





