package com.atguigu.spark.chapter05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Spark_Create_TopN01_Sec {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN01_Sec")

    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("E:\\尚硅谷\\502_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")

    // 用户访问对象
    val filedRdd: RDD[UserVisitAction] = fileRdd.map {
      line => {
        val lines: Array[String] = line.split("_")
        UserVisitAction(
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

    val acc: CategoryCountAccumulator = new CategoryCountAccumulator()

    sc.register(acc, "CategoryCountAccumulator")

    filedRdd.foreach {
      datas => {
        acc.add(datas)
      }
    }

    //3.5 获取累加器的值
    //((鞋,click),10)
    val res: mutable.Map[(String, String), Long] = acc.value

    println(">>>>>>>>>>>>>>>>>>>>"+res.size)

    // 对累加出来的数据按照类别进行分组，注意：这个时候每个类别之后有三条记录，数据量不会很大
    val groupRes: Map[String, mutable.Map[(String, String), Long]] = res.groupBy(rs =>rs._1._1)

    val cateGroup: immutable.Iterable[CategoryCountInfo] = groupRes.map {
      case (id, map) => {
        CategoryCountInfo(
          id,
          map.getOrElse((id, "click"), 0L),
          map.getOrElse((id, "order"), 0L),
          map.getOrElse((id, "pay"), 0L)
        )
      }
    }
    cateGroup.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(20).foreach(println)

  }
}

class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]] {

  var map = mutable.Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    var accumulator = new CategoryCountAccumulator
    accumulator.map = this.map
    accumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  // ((鞋,click),1)
  override def add(v: UserVisitAction): Unit = {
    if (v.click_category_id != -1) {

      val key = (v.click_category_id.toString, "click")
      map(key) = map.getOrElse(key, 0L) + 1L

    } else if (v.order_category_ids != "null") {
      // 订单
      val orders: Array[String] = v.order_category_ids.split(",")
      for (id <- orders) {
        val orderKey: (String, String) = (id, "order")
        map(orderKey) = map.getOrElse(orderKey, 0L) + 1L
      }
    } else if (v.pay_category_ids != "null") {
      val payIds: Array[String] = v.pay_category_ids.split(",")
      for (id <- payIds) {
        val payKey: (String, String) = (id, "pay")
        map(payKey) = map.getOrElse(payKey, 0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    val map1: mutable.Map[(String, String), Long] = map
    val map2: mutable.Map[(String, String), Long] = other.value

    map = map1.foldLeft(map2) {
      (mp2, kv) => {
        val keys: (String, String) = kv._1
        val values: Long = kv._2
        mp2(keys) = mp2.getOrElse(keys, 0L) + values
        mp2
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}