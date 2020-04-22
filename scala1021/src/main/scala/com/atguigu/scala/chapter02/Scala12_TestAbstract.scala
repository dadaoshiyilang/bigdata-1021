package com.atguigu.scala.chapter02

object Scala12_TestAbstract {
  def main(args: Array[String]): Unit = {
    var s:Person12 = new Student12
    println(s.age)
    println(s.name)
    s.eat()
    s.sleep()
  }
}

abstract class Person12 {
  // 非抽象属性
  // 如果重写父类中的属性，只能val修饰
  val name:String ="zhangsan"
  // 抽象属性
  var age:Int

  def  eat(): Unit ={
    println(" person eat")
  }

  // 抽象方法
  def sleep()
}

class Student12 extends Person12 {

  override var age: Int = 19

  override def sleep(): Unit = {
    println("sleep---")
  }

  override  val name:String ="lisi"


  override def eat(): Unit = {
    super.eat()
    println("子类重写的方法")
  }
}