package com.atguigu.scala.chapter06

object Scala_Array_Test {
  def main(args: Array[String]): Unit = {

    // 不可变数组
      val ints:Array[Int] = new Array[Int](5)

    ints(0) = 10
    ints.update(1, 20)
    println(ints(1))

    for (i <- 0 until ints.length ) {
      println(ints(i))
    }

    for (elem:Int <- ints) {
      println(elem)
    }

    val iterator: Iterator[Int] = ints.iterator
    while (iterator.hasNext) {
      println(iterator.next())
    }

    ints.foreach((a:Int)=>{
      println(a)
    })

    ints.foreach(println(_))

    ints.foreach(println)

    println(ints.mkString(","))


//    val newArr: Array[Int] = ints.+:(30)
//
//    println(ints.mkString(","))
//    println(newArr.mkString(","))

    val newArr2: Array[Int] = ints :+ 50


    println(ints.mkString(","))

    println(newArr2.mkString(","))

//    val arrays: Array[Int] = Array(1,2,3)
//
//    arrays.foreach(println)


    for(i <- 1 until 3) {
      println(i + " ")
    }
    println()

    for(i <- 1 to 3){
      println(i + " ")
    }
    println()
  }
}