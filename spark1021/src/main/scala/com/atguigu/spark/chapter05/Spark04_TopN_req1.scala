package com.atguigu.spark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Author: Felix
  * Date: 2020/3/14
  * Desc:  TopN需求一实现
  */
object Spark04_TopN_req1 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //1.读取数据
    val lineRDD: RDD[String] = sc.textFile("E:\\尚硅谷\\502_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")

    //2.将读取到的数据封装到UserVisitAction对象中
    val actionRDD: RDD[UserVisitAction] = lineRDD.map {
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    }
    //3.对当前UserVisitAction对象的RDD中的数据处理，得到CategoryCountInfo的RDD
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap {
      userAction => {
        if (userAction.click_category_id != -1) {
          //点击  因为我们使用的是flatMap，要求返回结果为可迭代的，所有放到List集合中包装一下，作为一个整体
          List(CategoryCountInfo(userAction.click_category_id + "", 1, 0, 0))
        } else if (userAction.order_category_ids != "null") { //注意：在比较是否为null的时候，null是字符串类型
          //下单
          val ids: Array[String] = userAction.order_category_ids.split(",")
          val orderList: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          for (id <- ids) {
            orderList.append(CategoryCountInfo(id, 0, 1, 0))
          }
          orderList
        } else if (userAction.pay_category_ids != "null") {
          //支付
          val ids: Array[String] = userAction.pay_category_ids.split(",")
          val payList: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          for (id <- ids) {
            payList.append(CategoryCountInfo(id, 0, 0, 1))
          }
          payList
        } else {
          Nil
        }
      }
    }


    //CategoryCountInfo(4,0,0,1)
    //CategoryCountInfo(4,1,0,1)
    //CategoryCountInfo(4,0,1,0) ==>CategoryCountInfo(4,1,1,1)
    //4.将相同的品类划分为一组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(
      info => info.categoryId
    )

    //5.对分组后的数据进行聚合处理
    val mapValueRDD: RDD[(String, CategoryCountInfo)] = groupRDD.mapValues {
      datas => {
        datas.reduce(
          (info1, info2) => {
            info1.clickCount = info1.clickCount + info2.clickCount
            info1.orderCount = info1.orderCount + info2.orderCount
            info1.payCount = info1.payCount + info2.payCount
            info1
          }
        )
      }
    }
    //6.再次转换结构  只保留CategoryCountInfo
    val mapRDD: RDD[CategoryCountInfo] = mapValueRDD.map(_._2)

    //7.取前10   倒序排序，因为我们这里有多个字段参与比较，所以推荐使用元组进行排序
    val resRDD: Array[CategoryCountInfo] = mapRDD
      .sortBy(info => (info.clickCount, info.orderCount, info.payCount), false)
      .take(20)

    resRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
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
case class CategoryCountInfo(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数


