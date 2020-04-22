package com.atguigu.spark.chapter04

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Create_Jdbc_Write {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_Jdbc_Write")

    val sc: SparkContext = new SparkContext(conf)

    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://localhost:3306/test"
    var username = "root"
    var passwd = "root"

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("qiaofeng",40),("xuzhu",35),("duanyu",30)))

    rdd.foreachPartition{
      //datas是表示的是一个分区中的数据
      datas=>{
        //注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url,username,passwd)
        //执行的SQL语句
        var sql:String = "insert into t_test(name,age) values(?,?)"
        //获取数据库操作对象
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //对当前分区内的数据再次进行遍历
        datas.foreach{
          case (name,age)=>{
            //给参数赋值
            ps.setString(1,name)
            ps.setInt(2,age)
            //执行SQL语句
            ps.executeUpdate()
          }
        }
        //释放资源
        ps.close()
        conn.close()
      }
    }
    sc.stop()
  }
}