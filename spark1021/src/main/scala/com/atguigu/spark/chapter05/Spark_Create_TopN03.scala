package com.atguigu.spark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Spark_Create_TopN03 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_TopN03")

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

    //计算分母-将相同的页面id进行聚合统计(pageId,sum)
    // (12,1)
    // (7,1)
    // (7,1)
    val pageIdRDD: RDD[(Long, Long)] = filedRdd.map(action => {
      (action.page_id, 1L)
    })

//    val fmIdsMap: Map[Long, Long] = pageIdRDD.reduceByKey(_+_).collect().toMap

    // (3,3672)
    //(7,3545)
    val reduceByKeyRdd: RDD[(Long, Long)] = pageIdRDD.reduceByKey(_+_)

    // (17,3646)
    // (3,3672)
    // (7,3545)
    val reduceByKeyCo: Array[(Long, Long)] = reduceByKeyRdd.collect()

    // (49,3548)
    // (7,3545)
    val toMap: Map[Long, Long] = reduceByKeyCo.toMap

//    toMap.foreach(println)


     //计算分子-将页面id进行拉链，形成连续的拉链效果，转换结构(pageId-pageId2,1)
     //将原始数据根据session进行分组
     val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = filedRdd.groupBy(_.session_id)

     //将分组后的数据根据时间进行排序（升序）
     var pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(datas => {
       val actions: List[UserVisitAction] = datas.toList.sortWith(
         (left, right) => {
           left.action_time < right.action_time
         }
       )
       //将排序后的数据进行结构的转换(pageId,1)
       val pageIdToOneList: List[(Long, Int)] = actions.map(action => (action.page_id, 1))


       //println(">>>>>>>>>>>>>>1111111111111111111>>>>>>>>>>>>")
       //pageIdToOneList.foreach(println)
       //println(">>>>>>>>>>>>>>>22222222222222222>>>>>>>>>>>")

       //pageIdToOneList.tail.foreach(println)

       // (9,1)
       // (7,1)


       // list02 (7,1)
       val pageFlows: List[((Long, Int), (Long, Int))] = pageIdToOneList.zip(pageIdToOneList.tail)
       //println(">>>>>>>>>>>>>>333333333333333>>>>>>>>>>>>")
       // ((9,1),(7,1))
       // pageFlows.foreach(println)
       //println(">>>>>>>>>>>>>>>44444444444444>>>>>>>>>>>")
       pageFlows.map {
         case ((pageId1, _), (pageId2, _)) => {
           (pageId1 + "-" + pageId2, 1)
         }
       }
     }
     )

     //将转换结构后的数据进行聚合统计(pageId-pageId2,sum)
    // println(">>>>>>>>>>>>>>>55555555>>>>>>>>>>>")
    // pageFlowRDD.map(_._2).foreach(println)
    // println(">>>>>>>>>>>>>>>6666666>>>>>>>>>>>")
    // pageFlowRDD: RDD[(String, List[(String, Int)])]
    // List((39-37,1), (37-21,1), (21-42,1), (42-36,1))
     val pageFlowMapRDD: RDD[(String, Int)] = pageFlowRDD.map(_._2).flatMap(list=>list)
    // (2-41,1)
    //(41-34,1)
    //(34-47,1)
    //(47-27,1)
     val page1AndPage2ToSumRDD: RDD[(String, Int)] = pageFlowMapRDD.reduceByKey(_+_)
    // (41-46,69)
    // (26-23,70)
    // (49-42,69)

     //计算页面单跳转换率
     page1AndPage2ToSumRDD.foreach{
       case (pageFlow,fz)=>{
         val pageIds: Array[String] = pageFlow.split("-")
         //为了避免分母不存在，这里默认值给1
         val fmSum: Long = toMap.getOrElse(pageIds(0).toLong,1L)
         println(pageFlow + "=" + fz.toDouble/fmSum)
       }
     }

    sc.stop()
  }
}