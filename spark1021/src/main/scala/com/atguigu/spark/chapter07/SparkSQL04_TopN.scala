package com.atguigu.spark.chapter07

import java.text.DecimalFormat
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Author: Felix
  * Date: 2020/3/17
  * Desc: 不同地区热门商品topN
  */
object SparkSQL04_TopN {
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val spark: SparkSession = SparkSession
      .builder
      .enableHiveSupport()
      .master("local[*]")
      .appName("SparkSQL04_TopN")
      .getOrCreate()

    spark.sql("use default")
    //创建自定义的UDAF函数对象
    val myUDAF: AreaClickUDAF = new AreaClickUDAF
    //注册自定义的聚合函数
    spark.udf.register("city_remark",myUDAF)

    //	查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与 Product_info 表连接得到产品名称
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
    //	按照地区和商品名称分组，统计出每个商品在每个地区的总点击次数
    spark.sql(
      """
        |select
        | t1.area,
        | t1.product_name,
        | count(*) as product_click_count,
        | city_remark(t1.city_name)
        |from
        |t1
        |group by t1.area,t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")
    //	每个地区内按照点击次数降序排列
    spark.sql(
      """
        |select
        | t2.*,
        | row_number() over (partition by t2.area order by t2.product_click_count desc) cn
        |from
        | t2
      """.stripMargin).createOrReplaceTempView("t3")
    //	只取前三名，并把结果保存在数据库中

    spark.sql(
      """
        |select
        | *
        |from
        |t3
        |where t3.cn <= 3
      """.stripMargin).show(false)
  }
}

/*
*   UDAF：聚合函数
*     select avg(age) from XXX
*     select area,product_name,count(*),city_remark(city_name) from TT group by area,product_name;
*/
class AreaClickUDAF extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("city_name",StringType)))
  }
  /*
  //缓存区域数据类型   北京->4,天津->2  |  总点击次数
    缓存中的第一个数据类型，应该是一个Map集合，存储的是 城市名称->当前城市点击次数
    缓存中的第二个数据类型，应该是一个Long, 存储的是总的点击数
  */
  override def bufferSchema: StructType = {
    StructType(Array(
            StructField("city_count",MapType(StringType,LongType)),
            StructField("total_count",LongType)
          ))
  }

  //输出数据类型 北京21.2%，天津13.2%，其他65.6%
  override def dataType: DataType = StringType

  //稳定性
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //map集合，存放的是城市对应的点击数
    buffer(0) = Map[String,Long]()
    //总的点击次数  初始化为0L
    buffer(1) = 0L
  }



  //更新缓存区
  //buffer  缓存      input 输入数据（城市名）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取当前输入的城市名称
    val cityName: String = input.getString(0)
    //获取缓存中的map集合
    //buffer.getAs(Map[String,Long])(0)
    val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)

    //根据城市的名称，到缓存中的map集合中查找该城市是否存在，如果存在的话，那么对应的值+1；
    // 不存在，将当前城市添加到Map集合中，key为城市名，value为1L
    //map(cityName) = map.getOrElse(cityName,0L) + 1L
    buffer(0) = map + (cityName->(map.getOrElse(cityName,0L) + 1L))
    //获取缓存中的总点击次数
    buffer(1) = buffer.getLong(1) + 1L
  }

  //合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var map1 = buffer1.getAs[Map[String,Long]](0)
    var map2 = buffer2.getAs[Map[String,Long]](0)

    //对map1和map2进行合并
    buffer1(0) = map1.foldLeft(map2){
      case (map,(k,v))=>{
        //map(k) = map.getOrElse(k,0L) + v
        //map
        map + (k->(map.getOrElse(k,0L) + v))
      }
    }

    //合并总的点击量
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //输出   北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): Any = {
    //从缓存中获取城市以及对应的点击次数
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    //从缓存中获取总点击次数
    val totalCount: Long = buffer.getLong(1)

    //对区域中的城市按照点击次数进行降序排序，取前两个
    val sortList: List[(String, Long)] = cityCountMap.toList.sortBy(-_._2).take(2)

    //计算排名在前2的城市的点击率
    var citysRatio: List[CityRemark] = sortList.map {
      case (cityName, count) => {
        CityRemark(cityName, count.toDouble / totalCount)
      }
    }

    //如果说，城市的个数点击率统计超过两个城市的话，那么我们只显示前2，剩下的作为其它的显示
    if(cityCountMap.size > 2){
      //citysRatio :+ CityRemark("其它", 1-citysRatio(0).cityRatio - citysRatio(1).cityRatio)
      citysRatio = citysRatio :+ CityRemark("其它", citysRatio.foldLeft(1D)(_-_.cityRatio))
    }
    citysRatio.mkString(",")
  }
}

case class CityRemark(cityName: String, cityRatio: Double) {
  val formatter = new DecimalFormat("0.00%")
  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}
