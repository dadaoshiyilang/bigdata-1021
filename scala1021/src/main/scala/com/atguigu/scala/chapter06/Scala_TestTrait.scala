package com.atguigu.scala.chapter06

object Scala_TestTrait {
  def main(args: Array[String]): Unit = {
    println(new MyOperation().describe())
  }
}

trait Operation {
  def describe():String ={
    "插入数据"
  }
}

trait DBOperation extends Operation {

  override def describe(): String = {
    "向DB中插入数据" + super.describe()
  }
}

trait HDFSOperation extends Operation {
  override def describe(): String = {
    "向HDFS中插入数据" + super.describe()
  }
}

class MyOperation extends DBOperation with HDFSOperation {
  override def describe(): String = "my operation is " + super.describe()
}
