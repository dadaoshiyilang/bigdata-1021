package com.atguigu.scala.chapter09

import scala.collection.mutable.ArrayBuffer

//泛型模板-不变
//class MyList[T]{}
// 协变
//class MyList[+T]

class MyList[-T]

class Parent{ }

class Child extends Parent { }

class SubChild extends Child { }

object TestGeneric {

  def main(args: Array[String]): Unit = {


    println("Hello".reverse(0))

    val b = ArrayBuffer[Int]()
    b +=1
    b.foreach(println)

    //println(Array(1, 7, 2, 9).sorted.foreach(println))


    println(Array("one", "two", "three").max)

    var tm = List(1,2,3)

    println(tm.foldLeft(0)(_ + _))
    println(tm.reduceLeft(_ + _))

    println((0 /: tm) (_ + _))
  }



}
