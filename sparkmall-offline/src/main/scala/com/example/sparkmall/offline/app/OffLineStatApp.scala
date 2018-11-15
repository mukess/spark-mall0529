package com.example.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.sparkmall.common.model.UserVisitAction
import com.example.sparkmall.common.util.ConfigurationUtil
import com.example.sparkmall.offline.resultmodel._
import com.example.sparkmall.offline.util.SessionStatAccumulate
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object OffLineStatApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OffLine").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val sessionStatAccumulate = new SessionStatAccumulate
    sparkSession.sparkContext.register(sessionStatAccumulate)

    val taskId = UUID.randomUUID().toString
    val conditionConfig = ConfigurationUtil("condition.properties").config
    val conditions = conditionConfig.getString("condition.params.json")

    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession)
    val userVisitActionSessionIdRDD: RDD[(String, UserVisitAction)] = userVisitActionRDD.map(userVisitAction => (userVisitAction.session_id, userVisitAction))
    val sessionActionItrRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionSessionIdRDD.groupByKey()
    val sessionIdInfoRDD = sessionActionItrRDD.map { case (sessionId, actionItr) =>
      var maxActionTime = 0L
      var minActionTime = 0L

      val searchkeywords = ListBuffer[String]()
      val clickProductIds = ListBuffer[String]()
      val orderProductIds = ListBuffer[String]()
      val payProductIds = ListBuffer[String]()

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      for (act <- actionItr) {
        val actionTime: Date = format.parse(act.action_time)
        val actionTimeMilSec: Long = actionTime.getTime

        if (minActionTime == 0L || actionTimeMilSec < minActionTime) {
          minActionTime = actionTimeMilSec
        }
        if (maxActionTime == 0L || actionTimeMilSec > maxActionTime) {
          maxActionTime = actionTimeMilSec
        }
        //合并相关信息

        if (act.search_keyword != null) searchkeywords += act.search_keyword
        if (act.click_product_id > 0) clickProductIds += act.click_product_id.toString
        if (act.order_product_ids != null) orderProductIds += act.order_product_ids
        if (act.pay_product_ids != null) payProductIds += act.pay_product_ids

      }
      val visitLength = (maxActionTime - minActionTime) / 1000
      println(s"visitLength = ${visitLength}")
      val stepLength = actionItr.size
      val startTimeString = format.format(new Date(minActionTime))

      val sessionInfo = SessionInfo(taskId, sessionId, visitLength, stepLength, startTimeString, searchkeywords.mkString(","), clickProductIds.mkString(","), orderProductIds.mkString(","), payProductIds.mkString(","))
      sessionInfo

      (sessionId, sessionInfo)

    }
    sessionIdInfoRDD.cache()


    //需求一///////////////////////////////////////
    import sparkSession.implicits._
    /*    val sessionStatRDD: RDD[SessionStat] = SessionStatApp.statSessionRatio(sparkSession, taskId, conditions, sessionStatAccumulate, sessionIdInfoRDD)
        insertMysql(sparkSession, sessionStatRDD.toDF, "session_stat")
        println(" 需求一保存完成！")*/

    //需求二///////////////////////////////////////
    /*    val sessionRDD: RDD[SessionInfo] = SessionRandomExtractor.extractSessions(sparkSession, sessionIdInfoRDD)
        insertMysql(sparkSession, sessionRDD.toDF(), "random_session_info")
        println(" 需求二保存完成！")*/

    //需求三///////////////////////////////////////
    /*    val catagoryCountMap: mutable.HashMap[String, Long] = CategoryCountApp.categoryCount(sparkSession, userVisitActionRDD)
//def groupBy [K] (f: (A) => K) : Map[K, Traversable[A]]
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
        val categoryTop10 = catagoryTopNs.toList.sortWith { (categoryTopN1, categoryTopN2) =>
          if (categoryTopN1.click_count < categoryTopN2.click_count) {
            true
          } else if (categoryTopN1.click_count == categoryTopN2.click_count) {
            if (categoryTopN1.order_count < categoryTopN1.order_count) {
              true
            } else {
              false
            }
          } else {
            false
          }
        }.take(10)
        val dataFrame: DataFrame = sparkSession.sparkContext.makeRDD(categoryTop10).toDF()*/
    /* insertMysql(sparkSession, dataFrame, "category_top10")
     println(" 需求三保存完成！")*/

    //需求四///////////////////////////////////////
    /*        val top10SessionPerCidRDD: RDD[TopSessionPerCid] = TopSessionPerTopCategoryApp.getTopSession(sparkSession, taskId, categoryTop10, userVisitActionRDD)
            insertMysql(sparkSession, top10SessionPerCidRDD.toDF(), "top10_session_per_top10_cid")
        println(" 需求四保存完成！")*/

    //需求五
    val pageConvertRates: List[PageConvertRate] = PageConvertRateApp.calcPageConvertRate(sparkSession, taskId, conditions, userVisitActionRDD)
    val pageConvertRatesRDD: RDD[PageConvertRate] = sparkSession.sparkContext.makeRDD(pageConvertRates)
    insertMysql(sparkSession, pageConvertRatesRDD.toDF(), "page_convert_rate")

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
