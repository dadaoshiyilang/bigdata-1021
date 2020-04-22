package com.atguigu.scala.chapter02

import com.atguigu.scala.chapter02.SingleTon.SingleTon1

object Scala_Test_Instance {
  def main(args: Array[String]): Unit = {
    val ins1: Unit = SingleTon.getInstance
    val ins2: Unit = SingleTon.getInstance
    if (ins1 == ins2) {
      println("相等")
    }
    val ins3: Unit = SingleTon1.getInstance
    val ins4: Unit = SingleTon1.getInstance
    if (ins3 == ins4) {
      println("相等")
    }
  }
}

class SingleTon private(){}

object SingleTon {
  private var s:SingleTon = null
  def getInstance ={
    if (s == null) {
      s = new SingleTon
    }
  }

  class SingleTon10 private() {

  }

  object SingleTon10 {
    private var s:SingleTon10 = null

    def getInstance():SingleTon10= {
      if (s == null) {
        s = new SingleTon10
      }
      return s
    }
  }


  class SingleTon1 private(){}

  object SingleTon1 {

    private val s: SingleTon1 = new SingleTon1

    def getInstance = {
      s
    }
  }
}

