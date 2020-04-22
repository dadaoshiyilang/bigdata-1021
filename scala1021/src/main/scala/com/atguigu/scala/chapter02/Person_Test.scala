package com.atguigu.scala.chapter02

class Person1245{

}


object Person_Test {

  def main(args: Array[String]): Unit = {

    val person = new Person1245

    //（1）判断对象是否为某个类型的实例
    val bool: Boolean = person.isInstanceOf  // alt + t

    if ( bool ) {
      //（2）将对象转换为某个类型的实例
      val p1: Person1245 = person.asInstanceOf[Person1245]
      val person124: Person1245 = person.asInstanceOf[Person1245]
      println(p1)
      println(person124)
    }

    //（3）获取类的信息
    val pClass: Class[Person1245] = classOf[Person1245]
    println(pClass)
    val value: Class[Person1245] = classOf[Person1245]

    val clazz: Class[_] = person.getClass

    println(clazz)
  }
}



