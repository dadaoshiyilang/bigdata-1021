package com.atguigu.scala.chapter02

object Scala_TestAbstract10 {

}

class Person {
  val name: String = "person"
  def hello(): Unit = {
    println("hello person")
  }
}

class Teacher extends Person {

  override val name: String = "teacher"

  override def hello(): Unit = {
    println("hello teacher")
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val teacher: Teacher = new Teacher()
    println(teacher.name)
    teacher.hello()

    val teacher1:Person = new Teacher
    println(teacher1.name)
    teacher1.hello()
  }
}
