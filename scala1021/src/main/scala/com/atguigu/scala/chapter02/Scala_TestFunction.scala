package com.atguigu.scala.chapter02

object Scala_TestFunction {

  def main(args: Array[String]): Unit = {

    def test(s:String*): Unit = {
      println(s)
    }

    def test01(name:String, sex:String*): Unit = {
      println("name:"+name+",sex:"+sex)
    }

    //test01("zhangsan", "男", "女")


    def f1(name:String): String = {
      return  "hello" + name
    }

    def f2(name:String): String = {
      "hello" + name
    }

    def f3(name:String): String = "hello " + name

    // println(f3("atguigu"))

    def f4(name:String) = "hello " + name
    //println(f4("atguigu"))

    def f5(name:String){
      "hello " + name
    }

    //println(f5("atguigu"))

    def f7() = "hello llll"

    //println(f7())
    //println(f7)

    def f8 = "hello 1234"

    //println(f8)

    def f9(f:(String)=>Unit): Unit = {
      f("atguigu")
    }

    def f10(s:String): Unit = {
      println("hello999 " + s)
    }

    //f9(f10)

    f9((s:String) => {
      println("hello9999 " + s)
    })

    f9((s:String) => {println("hello1 " + s)})

    f9((s) => {println("hello2 " + s)})

    f9(s => {println("hello3 " + s)})

    f9(s => println("hello4 " + s))

    f9(println(_))

    f9(println)



      try {
        for (elem <- 1 to 10) {
          println(elem)
          if (elem == 5) throw new RuntimeException
        }
      }catch {
        case e =>
      }
      println("正常结束循环")
    }
}
