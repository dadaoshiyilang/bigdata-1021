package com.atguigu.scala.chapter09

object Scala_TestGeneric {

  class Parent{}
  class Child extends Parent{}
  class SubChild extends Child{}

  def main(args: Array[String]): Unit = {

//    test(classOf[Parent])

    test01(classOf[Parent])
  }

  // 泛型通配符之上限
  def test[A <: Child](a:Class[A])={
    println(a)
  }

  //泛型通配符之下限
  def test01[A >: Child](a:Class[A]) = {
    println(a)
  }

}
