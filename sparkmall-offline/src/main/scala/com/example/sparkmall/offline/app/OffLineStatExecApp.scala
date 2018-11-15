package com.example.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.sparkmall.common.util.ConfigurationUtil
import com.example.sparkmall.model.UserVisitAction
import com.example.sparkmall.offline.resultmodel._
import com.example.sparkmall.offline.util.SessionStatAccumulate
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object OffLineStatExecApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OffLine").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val taskId = UUID.randomUUID().toString

    val conditionConfig = ConfigurationUtil("condition.properties").config
    val conditions = conditionConfig.getString("condition.params.json")

    //声明 注册累加器
    val sessionStatAccumulate = new SessionStatAccumulate
    sparkSession.sparkContext.register(sessionStatAccumulate)


    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession)

    val catagoryCountMap: mutable.HashMap[String, Long] = CategoryCountApp.categoryCount(sparkSession, userVisitActionRDD)

    val catagoryActionMap: Map[String, mutable.HashMap[String, Long]] = catagoryCountMap.groupBy { case (cid_actiontype, count) =>
      val cid: String = cid_actiontype.split("_")(0)
      cid
    }

    val catagoryTopNs: immutable.Iterable[CatagoryTopN] = catagoryActionMap.map { case (cid, categoryActionCount) =>
      val clickCount = categoryActionCount.getOrElse(cid + "_click", 0L)
      val orderCount = categoryActionCount.getOrElse(cid + "_order", 0L)
      val payCount = categoryActionCount.getOrElse(cid + "_pay", 0L)
      CatagoryTopN(taskId, cid, clickCount, orderCount, payCount)
    }
    val categoryTop10: List[CatagoryTopN] = catagoryTopNs.toList.sortWith { (categoryTopN1, categoryTopN2) =>
      if (categoryTopN1.click_count > categoryTopN2.click_count) {
        true
      } else if (categoryTopN1.click_count == categoryTopN2.click_count) {
        if (categoryTopN1.order_count > categoryTopN1.order_count) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }.take(10)

    val hasByTop10: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
      val pay_product_ids: String = userVisitAction.pay_product_ids
      var buysId: Array[String] = null
      if (null != pay_product_ids) {
        var buysId: Array[String] = pay_product_ids.split(",")
        val topicidList: List[String] = categoryTop10.map(catagoryTop => catagoryTop.category_id)
        val inter: Array[String] = buysId.intersect(topicidList)
        inter.size != 0
      } else {
        false
      }

    }

    // hasByTop10.foreach(println)

    val visitTrace = hasByTop10.flatMap { userVisitAction =>
      val ops: mutable.ArrayOps[String] = userVisitAction.pay_product_ids.split(",")
      var userList = ArrayBuffer[(String, Long)]()
      ops.map(o =>
        if (categoryTop10.map(_.category_id).contains(o)) {
          userList.append((o + "_" + userVisitAction.user_id, 1L))

        }
      )
      userList
    }
    //visitTrace.foreach(println)
    val reduceVisitTrace: RDD[(String, Long)] = visitTrace.reduceByKey(_ + _)

    val productUserCount: RDD[(String, (String, Long))] = reduceVisitTrace.map { case (userIdAndProductId, count) =>
      val userId: String = userIdAndProductId.split("_")(0)
      val productId: String = userIdAndProductId.split("_")(1)

      (productId, (userId, count))
    }
    val productUserCountGroup: RDD[(String, Iterable[(String, Long)])] = productUserCount.groupByKey()
    val productTop2: RDD[(String, (String,Long))] = productUserCountGroup.flatMap { case (productId, iter) =>
      iter.toList.sortWith((it1, it2) => {

        it1._2 > it2._2
      }).take(2).map((productId,_))
    }


    productTop2.flatMap { case (productId, itr) =>
      val tuples: ArrayBuffer[(String, (String, Long))] = new ArrayBuffer[(String, (String, Long))]()
      itr
      tuples

    }


    //练习二///////////////////////////////////////
    /*    练习需求：
        求： 列出购买过10大热门商品的较多的用户的双十一当日的访问记录
        注： 每个热门商品选2个购买该商品最多的用户，列出他们当日所有访问明细用时间排序
        热门商品以点击量为准。
        结果表字段：
        热门商品id  商品名称  用户id 用户名称  访问时间  操作类型  操作商品id或搜索关键字
        操作类型:click,search,order,pay 四个之一*/


  }

  def insertMysql(sparkSession: SparkSession, dataFrame: DataFrame, tableName: String): Unit = {
    val config = ConfigurationUtil("config.properties").config

    dataFrame.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .option("dbtable", tableName).save()

  }

  def readUserVisitActionRDD(sparkSession: SparkSession) = {

    val configProperties = ConfigurationUtil("config.properties").config
    val conditionConfig = ConfigurationUtil("condition.properties").config
    val conditionJsonString = conditionConfig.getString("condition.params.json")
    val conditionJSonObj = JSON.parseObject(conditionJsonString)

    val sql = new StringBuilder("select ua.* from user_visit_action ua join user_info ui on ua.user_id=ui.user_id")
    sql.append(" where 1=1 ")
    if (conditionJSonObj.getString("startDate") != "") {
      sql.append(" and date>='" + conditionJSonObj.getString("startDate") + "'")
    }
    if (conditionJSonObj.getString("endDate") != "") {
      sql.append(" and date<='" + conditionJSonObj.getString("endDate") + "'")
    }
    if (conditionJSonObj.getString("startAge") != "") {
      sql.append(" and age>=" + conditionJSonObj.getString("startAge"))
    }
    if (conditionJSonObj.getString("endAge") != "") {
      sql.append(" and age<=" + conditionJSonObj.getString("endAge"))
    }
    if (conditionJSonObj.getString("professionals") != "") {
      sql.append(" and professionals=" + conditionJSonObj.getString("professionals"))
    }
    if (conditionJSonObj.getString("gender") != "") {
      sql.append(" and gender=" + conditionJSonObj.getString("gender"))
    }
    if (conditionJSonObj.getString("city") != "") {
      sql.append(" and city_id=" + conditionJSonObj.getString("city"))
    }
    val database = configProperties.getString("hive.database")
    if (database != null) {
      sparkSession.sql("use " + database)
    }
    println(s"sql.toString() = ${sql.toString()}")
    val dataFrame = sparkSession.sql(sql.toString())
    dataFrame.show(100)
    import sparkSession.implicits._
    dataFrame.as[UserVisitAction].rdd


  }
}
