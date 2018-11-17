package com.example.sparkmall.offline.app

import java.text.SimpleDateFormat

import com.example.sparkmall.offline.resultmodel.PageConvertRate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.JSON
import com.example.sparkmall.common.model.UserVisitAction
import org.apache.spark.broadcast.Broadcast

object PageConvertRateApp {

  def calcPageConvertRate(sparkSession: SparkSession, taskId: String, condition: String, userVisitActionRDD: RDD[UserVisitAction]): List[PageConvertRate] = {

    val conditionObj = JSON.parseObject(condition)
    val pageFlowArray: Array[String] = conditionObj.getString("targetPageFlow").split(".")
    val pageFlowArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageFlowArray.slice(0, 6))

    val pageCountMap: collection.Map[Long, Long] = userVisitActionRDD.filter(userVisitAction =>
      pageFlowArrayBC.value.contains(userVisitAction.page_id)
    ).map(userVisitAction =>
      (userVisitAction.page_id, 1L)
    ).countByKey()

    val pageFlowArrayPrefix: Array[String] = pageFlowArray.slice(0, 6)
    val pageFlowArraySuffix: Array[String] = pageFlowArray.slice(1, 7)

    val tuples: Array[(String, String)] = pageFlowArrayPrefix.zip(pageFlowArraySuffix)
    val targetPageJumps: Array[String] = tuples.map { case (prefix, suffix) => prefix + "_" + suffix }
    val targetPageJumpsBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageJumps)

    val sessionActionsRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map(userVisitAction =>
      (userVisitAction.session_id, userVisitAction)
    ).groupByKey()

    val pageJumpsRDD: RDD[(String, Long)] = sessionActionsRDD.flatMap { case (sessionId, itr) =>
      val actionsSorted: List[UserVisitAction] = itr.toList.sortWith { (item1, item2) =>
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        dateFormat.parse(item1.action_time).getTime < dateFormat.parse(item2.action_time).getTime
      }
      val actionZip: List[(UserVisitAction, UserVisitAction)] = actionsSorted.slice(0, actionsSorted.length - 1).zip(actionsSorted.slice(1, actionsSorted.length))

      val sessionPageJumps: List[String] = actionZip.map { case (act1, act2) =>
        act1.page_id + "_" + act2.page_id
      }
      val sessionPageJumpsFiltered: List[(String, Long)] = sessionPageJumps.filter(pageJumps =>
        targetPageJumpsBC.value.contains(pageJumps)
      ).map(pageJump => (pageJump, 1L))

      sessionPageJumpsFiltered

    }
    val pageJumpsCountMap: collection.Map[String, Long] = pageJumpsRDD.countByKey()

    val pageConvertRateList: List[PageConvertRate] = pageJumpsCountMap.map { case (pageJump, count) =>
      val fromPage: String = pageJump.split("_")(0)
      val fromPageCount: Long = pageCountMap.get(fromPage.toLong).get
      val pageJumpRate = Math.round(count / fromPageCount.toDouble * 1000) / 10.0
      PageConvertRate(taskId, pageJump, pageJumpRate)
    }.toList
    pageConvertRateList

  }

}
