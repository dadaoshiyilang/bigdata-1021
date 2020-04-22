package com.atguigu.scala.chapter09

class User(val name: String, val age: Int)

object User{
  def apply(name: String, age: Int): User = new User(name, age)
  def unapply(user: User): Option[(String, Int)] = {
    if (user == null)
      None
    else
      Some(user.name, user.age)
  }
}

case class Person(name: String, age: Int)

object TestMatchUnapply {

  def main(args: Array[String]): Unit = {
    val user: User = User("zhangsan", 11)
    val result = user match {
      case User("zhangsan", 11) => "yes"
      case _ => "no"
    }
    println(result)



    val (x, y) = (1, 2)
    println(s"x=$x,y=$y")

    val Array(first, second, _*) = Array(1, 7, 2, 9)
    println(s"first=$first,second=$second")

    val Person(name, age) = Person("zhangsan", 16)
    println(s"name=$name,age=$age")

  }

}
