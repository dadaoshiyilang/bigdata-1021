package com.atguigu.spark.chapter05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Spark_Create_TopN02_Sec {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN02_Sec")

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

    // CategoryCountInfo(15,0,0,1)
    // CategoryCountInfo(1,0,0,1)
    val flatMapRdd: RDD[CategoryCountInfo] = filedRdd.flatMap {
      userVist => {
        if (userVist.click_category_id != -1) {
          List(CategoryCountInfo(userVist.click_category_id.toString, 1, 0, 0))
        } else if (userVist.order_category_ids != "null") {
          val orderLists: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          val ids: Array[String] = userVist.order_category_ids.split(",")
          for (elem <- ids) {
            orderLists.append(CategoryCountInfo(elem, 0, 1, 0))
          }
          orderLists
        } else if (userVist.pay_category_ids != "null") {
          val listPays: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          val ids: Array[String] = userVist.pay_category_ids.split(",")
          for (elem <- ids) {
            listPays.append(CategoryCountInfo(elem, 0, 0, 1))
          }
          listPays
        } else {
          Nil
        }
      }
    }

    // 将相同的key分配到一组
    // (4,CompactBuffer(CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,1,0), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,1,0), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,0,1), CategoryCountInfo(4,0,1,0), CategoryCountInfo(4,0,1,0))
    val groupRdd: RDD[(String, Iterable[CategoryCountInfo])] = flatMapRdd.groupBy(elem => elem.categoryId)

    // (4,CategoryCountInfo(4,0,0,1271))
    // (8,CategoryCountInfo(8,0,1,1238))
    val infoRdd: RDD[(String, CategoryCountInfo)] = groupRdd.mapValues {
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
    val mapRDD: RDD[CategoryCountInfo] = infoRdd.map(_._2)

    val sortRdd: Array[CategoryCountInfo] = mapRDD.sortBy(info =>
      (info.clickCount, info.orderCount, info.payCount), false).take(10)

    val ids: Array[String] = sortRdd.map(_.categoryId)

    // //因为这个ids要分给每个任务，所以可以使用广播变量
    val brods: Broadcast[Array[String]] = sc.broadcast(ids)

    val filterRdd: RDD[UserVisitAction] = filedRdd.filter(
          fil => {
            if (fil.click_category_id != -1) {
              // 判断ids是否包含此id
              brods.value.contains(fil.click_category_id.toString)
//              ids.contains(fil.click_category_id.toString)
            } else {
               false
            }
          }
    )

//    val filterRdd: RDD[UserVisitAction] = filedRdd.filter(
//      action => {
//        if (action.click_category_id != -1) {//只保留点击操作
//          //只保留热门品类  ids字符串数组，click_category_id是Long类型，始终包含不了
//          ids.contains(action.click_category_id.toString)
//        } else {
//          false
//        }
//      }
//    )

    val filMapRdd: RDD[(String, Int)] = filterRdd.map {
      mp => {
        (mp.click_category_id + "_" + mp.session_id, 1)
      }
    }

    val reduceKeyRdd: RDD[(String, Int)] = filMapRdd.reduceByKey(_+_)

    val reduMapRdd: RDD[(String, (String, Int))] = reduceKeyRdd.map {
      mp => {
        val splits: Array[String] = mp._1.split("_")
        (splits(0), (splits(1), mp._2))
      }
    }

    val groupKeyRdd: RDD[(String, Iterable[(String, Int)])] = reduMapRdd.groupByKey()


//    val testRdd: RDD[(String, (String, Int))] = reduceKeyRdd.map {
//      case (words, cnt) => {
//        val splits: Array[String] = words.split("_")
//        (splits(0), (splits(1), cnt))
//      }
//    }

    val resRdd: RDD[(String, List[(String, Int)])] = groupKeyRdd.mapValues {
      mp => {
        mp.toList.sortWith(
          (x, y) => {
            x._2 > y._2
          }
        )
      }.take(4)
    }

    resRdd.foreach(println)
    sc.stop()
  }
}