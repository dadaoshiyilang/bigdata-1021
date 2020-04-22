package com.atguigu.scala.chapter02

/**
  * Scala注释
  * @author lz
  */
object Scala01_TestComment {
  def main(args: Array[String]): Unit = {

    for(i <- 1 to 9) {
      for (j <- 1 to i) {
        print(j + "*" +i+ "="+(i*j) +"\t")
      }
      println()
    }

    for(i <- 1 to 9) {
      println("*"*(2*i -1)+"\t")
    }

    for (i <- Range(1, 18, 2); j = (18 - i) / 2) {
      println(" " * j + "*" * i + " " * j)
    }


  }
}
