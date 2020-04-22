package com.atguigu.scala.chapter07

import scala.collection.mutable

object Scala_Test_Map01 {

  def main(args: Array[String]): Unit = {

    // 两个Map的数据合并
    val map1: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
    val map2: mutable.Map[String, Int] = mutable.Map("a"->4, "b"->5, "d"->6)

    // val map3: mutable.Map[String, Int] = map1.map(s=>{s._1->(s._2 + map2.getOrElse(s._1,0))})

    // println(map3)

    val map3: mutable.Map[String, Int] = map2.foldLeft(map1) {
      (map, kv) => {
        val k = kv._1
        val v = kv._2

        map(k) = map.getOrElse(k, 0) + v

        map
      }
    }

    println(map3)
  }
}
