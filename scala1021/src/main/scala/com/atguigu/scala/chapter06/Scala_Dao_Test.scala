package com.atguigu.scala.chapter06

object Scala_Dao_Test extends APP with Dao {

  def main(args: Array[String]): Unit = {
    login(new User("bobo", 11))
  }
}

class User (val name:String, val age:Int)

trait Dao {
  def insert(user:User) ={
    println("insert into database: " + user.name)
  }
}

trait APP {
  _: Dao =>
  def login(user:User)={
    println("login :" + user.name)
    insert(user)
  }
}
