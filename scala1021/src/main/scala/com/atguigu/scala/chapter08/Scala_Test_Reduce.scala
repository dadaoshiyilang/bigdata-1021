package com.atguigu.scala.chapter08

object Scala_Test_Reduce {

  def main(args: Array[String]): Unit = {

    val list01: List[Int] = List(1,2,3,4)

    println("将数据两两结合，实现加法运算："+list01.reduce((x,y) => x + y))

    println("将数据两两结合，实现减法运算："+list01.reduce((x,y) => x - y))

    println("reduceleft：reduce底层调用的就是这："+list01.reduceLeft((x, y) => x - y))

    println("reduceleft：reduce底层调用的就是这："+list01.reduceLeft((x, y) => y - x))

    // reduceRight的底层实现：reversed.reduceLeft[B]((x, y) => op(y, x))
    // 1,2,3,4 ==> 4,3,2,1
    println("reduceright:实现数据的相减："+list01.reduceRight((x, y) => x - y))

    // 1,2,3,4 ==> 4,3,2,1
    println("reduceright:实现数据的相减："+list01.reduceRight((x, y) => y - x))

  }

}
