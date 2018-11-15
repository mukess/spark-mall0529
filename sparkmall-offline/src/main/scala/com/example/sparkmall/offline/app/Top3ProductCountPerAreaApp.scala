package com.example.sparkmall.offline.app

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//未完成
object Top3ProductCountPerAreaApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OffLine").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val taskId = UUID.randomUUID().toString

    sparkSession.sql("use sparkmall")
    val df: DataFrame = sparkSession.sql("select * from user_visit_action ua join city_info ci on ua.city_id = ci.city_id " +
      "where click_product_id > 0")
    df.createTempView("tmp_area_product_click")

    sparkSession.sql("select area,click_product_id,count(*) clickcount,groupby_city_count")
  }
}
