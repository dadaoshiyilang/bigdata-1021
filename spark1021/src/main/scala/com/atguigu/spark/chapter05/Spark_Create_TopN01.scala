package com.atguigu.spark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Spark_Create_TopN01 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN01")

    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("E:\\尚硅谷\\502_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")

    // 用户访问对象
    val filedRdd: RDD[UserVisitAction1] = fileRdd.map {
      line => {
        val lines: Array[String] = line.split("_")
        UserVisitAction1(
          lines(0),
          lines(1).toLong,
          lines(2),
          lines(3).toLong,
          lines(4),
          lines(5),
          lines(6).toLong,
          lines(7).toLong,
          lines(8),
          lines(9),
          lines(10),
          lines(11),
          lines(12).toLong
        )
      }
    }

    // CategoryCountInfo(15,0,0,1)
    // CategoryCountInfo(1,0,0,1)
    val flatMapRdd: RDD[CategoryCountInfo1] = filedRdd.flatMap {
      userVist => {
        if (userVist.click_category_id != -1) {
          List(CategoryCountInfo1(userVist.click_category_id.toString, 1, 0, 0))
        } else if (userVist.order_category_ids != "null") {
          val orderLists: ListBuffer[CategoryCountInfo1] = new ListBuffer[CategoryCountInfo1]
          val ids: Array[String] = userVist.order_category_ids.split(",")
          for (elem <- ids) {
            orderLists.append(CategoryCountInfo1(elem, 0, 1, 0))
          }
          orderLists
        } else if (userVist.pay_category_ids != "null") {
          val listPays: ListBuffer[CategoryCountInfo1] = new ListBuffer[CategoryCountInfo1]
          val ids: Array[String] = userVist.pay_category_ids.split(",")
          for (elem <- ids) {
            listPays.append(CategoryCountInfo1(elem, 0, 0, 1))
          }
          listPays
        } else {
          Nil
        }
      }
    }

    // 将相同的key分配到一组
    // (4,CompactBuffer(CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,1,0), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,1,0), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,1,0), CategoryCountInfo(4,0,1,0))
    val groupRdd: RDD[(String, Iterable[CategoryCountInfo1])] = flatMapRdd.groupBy(elem => elem.categoryId)

    // (4,CategoryCountInfo(4,0,0,1271))
    // (8,CategoryCountInfo(8,0,1,1238))
    val infoRdd: RDD[(String, CategoryCountInfo1)] = groupRdd.mapValues {
      datas =>
        datas.reduce (
          (info1, info2) => {
            info1.clickCount = info1.clickCount + info2.clickCount
            info1.orderCount = info1.orderCount + info2.clickCount
            info1.payCount = info1.payCount + info2.payCount
            info1
          }
        )
    }

    // (4,CategoryCountInfo(4,0,0,1271)) => CategoryCountInfo(4,0,0,1271)
    // CategoryCountInfo(8,0,1,1238)
    val mapRDD: RDD[CategoryCountInfo1] = infoRdd.map(_._2)

    val sortRdd: Array[CategoryCountInfo1] = mapRDD.sortBy(info =>
      (info.clickCount, info.orderCount, info.payCount), false).take(20)

    sortRdd.foreach(println)
    sc.stop()
  }
}

case class UserVisitAction1(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo1(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数