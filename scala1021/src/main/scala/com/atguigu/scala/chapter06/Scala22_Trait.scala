package com.atguigu.scala.chapter06

object Scala22_Trait {
  def main(args: Array[String]): Unit = {
     val clazz = new MyClass
  }
}

trait TraitA22 {
  println("特质的构造方法")
  def ml():Unit
}

abstract  class Ab {
  println("抽象类的构造方法")
  def m2():Unit
}

// 抽象类和特质都不能实例化
// 抽象类和特质都有抽象属性和抽象方法
// 抽象类中可以定义带参构造方法，特质中不允许

class MyClass extends Ab with TraitA22 {

  override def ml(): Unit = {

  }

  override def m2(): Unit = {

  }
}