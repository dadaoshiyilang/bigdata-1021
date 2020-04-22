package com.atguigu.spark.chapter07

import java.text.DecimalFormat
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object SparkSQL_HIve_ToPN_UDAF {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().master("local[*]")
      .appName("SparkSQL_HIve_ToPN_UDAF").getOrCreate()

    spark.sql("use default")

    val myTopN: topN_UDAF = new topN_UDAF

    spark.udf.register("topN_UDAF", myTopN)

    // 查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与 Product_info 表连接得到产品名称
    spark.sql(
      """
        |select
        | c.*,
        | p.product_name
        |from
        | user_visit_action a
        |join
        | city_info c
        |on
        | a.city_id = c.city_id
        |join
        | Product_info p
        |on
        | a.click_product_id = p.product_id
        |where
        | a.click_product_id != -1
      """.stripMargin).createOrReplaceTempView("t1")

    // 按照地区和商品 id 分组，统计出每个商品在每个地区的总点击次数
    spark.sql(
      """
        |select
        | t1.area,
        | t1.product_name,
        | count(*) as product_click_count,
        | topN_UDAF(t1.city_name)
        |from
        | t1
        |group by t1.area,t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")
    //每个地区内按照点击次数降序排列
    spark.sql(
      """
        | select
        | t2.*,
        | row_number() over( partition by t2.area order by t2.product_click_count desc) cn
        |from
        | t2
      """.stripMargin).createOrReplaceTempView("t3")
    //只取前三名，并把结果保存在数据库中
    spark.sql(
      """
        | select
        | *
        | from
        | t3
        | where t3.cn <= 3
      """.stripMargin).show(false)



  }
}

class topN_UDAF extends  UserDefinedAggregateFunction {

  override def inputSchema: StructType = {
    StructType(Array(StructField("city_name", StringType)))
//    StructType(StructField("city_name", StringType) :: Nil)
  }

  // 缓存区数据类型 北京 ->6,上海 ->6 | 总点击次数
  // 缓存中第一个数据类型：应该是一个map集合
  // 第二个数据类型：应该是一个Long
  override def bufferSchema: StructType = {
         StructType(Array(StructField("city_count", MapType(StringType, LongType)),
           StructField("total_count", LongType)
    ))
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  // 更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取当前的城市信息
    val cityName: String = input.getString(0)
    // 得到第一缓冲区城市map集合信息
//    val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    buffer(0) = map + (cityName ->(map.getOrElse(cityName, 0L) + 1L))
    // 城市点击量加1
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    var map1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    var map2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)

    buffer1(0) = map1.foldLeft(map2) {
      case (mp2, (k, v)) => {
        mp2 + (k -> (mp2.getOrElse(k, 0L) + v))
      }
    }

    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {

    // 获取map集合城市点击信息
    val cityCountMap1: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    // 总点击量
    val cnt: Long = buffer.getLong(1)

    // 每个区域
    val citySortby: List[(String, Long)] = cityCountMap1.toList.sortBy(-_._2).take(2)

    var cityRadio: List[cityRemark] = citySortby.map {
      case (cityName, count) => {
        cityRemark(cityName, count.toDouble/cnt)
      }
    }

    if (cityCountMap1.size > 2) {
      cityRadio = cityRadio :+ cityRemark("其它",cityRadio.foldLeft(1D)(_-_.cityRatio))
    }
    cityRadio.mkString(",")
  }
}

case class cityRemark(cityName:String, cityRatio:Double) {
   val format = new DecimalFormat("0.00%")
  override  def toString:String =s"$cityName:${format.format(cityRatio)}"
}