package com.atguigu.scala.chapter08

object Scala_Test_List {

  def main(args: Array[String]): Unit = {

    val list01: List[Int] = List(1, 3, 5, -3, 4, 2, -7 )

    println("求和："+ list01.sum)

    println("乘积："+ list01.product)

    println("最大值："+ list01.max)

    println("最小值："+ list01.min)

    println("按照元素大小排序："+list01.sortBy(x=>x))

    println("按照元素的绝对值排序："+list01.sortBy(x=>x.abs))

    println("按照元素的大小降序排序："+list01.sortWith((a,b)=> {a > b}))

    println("按照元素的大小升序排序："+list01.sortWith( (a,b) => a < b))

    println("sorted:对集合进行自然排序："+list01.sorted)


  }

}
