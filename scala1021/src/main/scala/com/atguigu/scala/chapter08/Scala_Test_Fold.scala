package com.atguigu.scala.chapter08

import scala.collection.mutable

object Scala_Test_Fold {

  def main(args: Array[String]): Unit = {

    val list01: List[Int] = List(1, 2, 3, 4)


    println("fold:"+list01.fold(0)(_ +_))
    println("foldleft:"+list01.foldLeft(1)((x,y)=>x+y))
    println("foldleft:"+list01.foldLeft(1)((x,y)=>x-y))

    //  reverse.foldLeft(z)((right, left) => op(left, right))
    // 1, 2, 3, 4 => 4,3,2,1
    println("foldright:"+list01.foldRight(10)((x,y)=> x-y))

    // 两个集合合并
    val map01 = mutable.Map("a"->1, "b"->2, "c"->3)
    val map02: mutable.Map[String, Int] = mutable.Map("a"->4, "b"->5, "d"->6)

//    val map001 = map01.foldLeft(map02)((mp, kv) => {
//      val key: String = kv._1
//      // println("--key--" + key)
//      val value: Int = kv._2
//      // println("--mp.getOrElse(key, 0)--" + mp.getOrElse(key, 0))
//      mp(key) = mp.getOrElse(key, 0) + value
//      mp
//    })
//    println(map001)

    val mao002 = map02.foldLeft(map01)((mp, kv) => {
      val key: String = kv._1
      println("--key--" + key)
      val value: Int = kv._2
      // println("--mp.getOrElse(key, 0)--" + mp.getOrElse(key, 0))
      mp(key) = mp.getOrElse(key, 0) + value
      mp
    })
    println(mao002)

  }

}
