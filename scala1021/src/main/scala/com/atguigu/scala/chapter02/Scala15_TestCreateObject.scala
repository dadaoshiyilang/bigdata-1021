package com.atguigu.scala.chapter02

object Scala15_TestCreateObject {

  def main(args: Array[String]): Unit = {
    val stud:Student15 = Student15()
    println(stud.name)
  }



}
object Student15 {
  def apply(): Student15 = new Student15()
}



class  Student15 private() {

  var name:String= _

  var age:Int = _

}

