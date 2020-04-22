package com.atguigu.spark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Author: Felix
  * Date: 2020/3/14
  * Desc:  TopN需求二实现
  */
object Spark05_TopN_req2 {
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
      .take(10)

    //需求二
    //	通过需求1，获取TopN热门品类的id
    val ids: Array[String] = resRDD.map(_.categoryId)

    //	将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {//只保留点击操作
          //只保留热门品类  ids字符串数组，click_category_id是Long类型，始终包含不了
          ids.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )

    //	对session的点击数进行转换 (category-session,1)
    val mapRDD1: RDD[(String, Int)] = filterRDD.map(
      action => (action.click_category_id + "_" + action.session_id, 1)
    )

    //	对session的点击数进行统计 (category-session,sum)
    val reduceRDD1: RDD[(String, Int)] = mapRDD1.reduceByKey(_+_)

    //	将统计聚合的结果进行转换  (category,(session,sum))
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD1.map {
      case (categoryAndSession, sum) => {
        val categoryAndSessionFields: Array[String] = categoryAndSession.split("_")
        (categoryAndSessionFields(0), (categoryAndSessionFields(1), sum))
      }
    }

    //	将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()
    //	对分组后的数据降序 取前10
    val resRDD2: RDD[(String, List[(String, Int)])] = groupRDD2.mapValues {
      datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }.take(4)
      }
    }

    resRDD2.foreach(println)


    // 关闭连接
    sc.stop()
  }
}