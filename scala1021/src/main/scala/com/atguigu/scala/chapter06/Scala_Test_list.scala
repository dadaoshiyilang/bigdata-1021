package com.atguigu.scala.chapter06

object Scala_Test_list {

  def main(args: Array[String]): Unit = {

    // 不可变list
    // 有序，可重复 immutable list->抽象类
    val lists: List[Int] = List(1,2,3,4,5)

    //向集合中添加元素
    val lists01: List[Int] = lists.+:(9)
    val list02: List[Int] = lists.::(50)

    val lists03: List[Int] = 60 :: lists

    // 向list中添加元素


    println(lists)
    println(lists01)
    println(list02)
    println(lists03)

    val list04: List[Int] = 10::20::30::40::Nil

    val list05: List[Int] = Nil.:: (10).::(20).::(30).::(40).::(50)

    println(list04)
    println(list05)

    val list06: List[Int] = List(1,2,3,4,5)

    val list07: List[Int] = List(7,8)

    val list08: List[Int] = list06:::list07

    println(list08)

    // 遍历集合元素
    //lists.foreach(println)
    lists01.foreach(println)

    //lists.foreach((a:Int)=>{println(a)})
  }

}
