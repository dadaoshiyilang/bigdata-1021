package com.atguigu.spark.chapter07

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL01_HIve_ToPN {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_HIve_ToPN")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与 Product_info 表连接得到产品名称
    spark.sql(
      """
        |select
        | c.area,
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
        | count(*) as product_click_count
        |from
        |(
        |   select
        |    c.area,
        |    p.product_name
        |   from
        |    user_visit_action a
        |   join
        |    city_info c
        |   on
        |    a.city_id = c.city_id
        |   join
        |    Product_info p
        |   on
        |    a.click_product_id = p.product_id
        |   where
        |    a.click_product_id != -1
        |)t1
        |group by t1.area,t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")
    //每个地区内按照点击次数降序排列
    spark.sql(
      """
        | select
        | t2.area,
        | t2.product_name,
        | t2.product_click_count,
        | row_number() over (partition by t2.area order by t2.product_click_count desc) cn
        |from
        | t2
      """.stripMargin).createOrReplaceTempView("t3")
    //只取前三名，并把结果保存在数据库中
    spark.sql(
      """
        | select
        | t3.area,
        | t3.product_name,
        | t3.product_click_count,
        | t3.cn
        | from
        | t3
        | where t3.cn <= 3
      """.stripMargin).show()
  }
}