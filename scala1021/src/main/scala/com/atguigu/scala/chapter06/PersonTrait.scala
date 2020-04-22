package com.atguigu.scala.chapter06

object PersonTrait {
  def main(args: Array[String]): Unit = {
    val teacher = new Teacher

    teacher.say()
    teacher.eat()
    println(teacher.age)

    val t2 = new Teacher with SexTrait {
      override var sex: String = _
    }
    println(t2.sex)
  }
}

trait PersonTrait1 {

  var name:String = _

  var age:Int

  def eat()={
    println("eat")
  }

  def say() :Unit
}

trait  SexTrait {
  var sex:String
}

class Teacher extends PersonTrait1 with java.io.Serializable {
  override def say(): Unit = {
    println("say")
  }

  override var age: Int = _
}

