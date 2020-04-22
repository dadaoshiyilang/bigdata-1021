package com.atguigu.scala.chapter02

object Scala14_TestNon {

  def main(args: Array[String]): Unit = {
    var p:Person14 = new Person14 {
      override var name: String = "sss"

      override def ml: Unit = {

      }
    }
  }
}



abstract class  Person14{
  var name:String

  def ml: Unit

}



