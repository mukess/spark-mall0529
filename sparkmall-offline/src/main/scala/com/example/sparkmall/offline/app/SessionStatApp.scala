package com.example.sparkmall.offline.app

import com.example.sparkmall.offline.resultmodel.{SessionInfo, SessionStat}
import com.example.sparkmall.offline.util.SessionStatAccumulate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SessionStatApp {
  def statSessionRatio(sparkSession: SparkSession, taskId: String, conditions: String, sessionStatAccumulate: SessionStatAccumulate, sessionIdInfoRDD: RDD[(String, SessionInfo)]) = {

    sessionIdInfoRDD.foreach { case (sessionId, sessionInfo) => {

      if (sessionInfo.visitLength <= 10L) {
        sessionStatAccumulate.add("visit_length_le_10s")
      } else {
        sessionStatAccumulate.add("visit_length_gt_10s")
      }

      if (sessionInfo.stepLength < 5L) {
        sessionStatAccumulate.add("step_length_le_5")
      } else {
        sessionStatAccumulate.add("step_length_gt_5")
      }
      sessionStatAccumulate.add("session_count")
    }


    }
    val sessionStatMap: mutable.HashMap[String, Long] = sessionStatAccumulate.value

    val visitLenLe10 = sessionStatMap.getOrElse("visit_length_le_10s", 0L)
    val visitLenGt10 = sessionStatMap.getOrElse("visit_length_gt_10s", 0L)
    val stepLenLe5 = sessionStatMap.getOrElse("step_length_le_5", 0L)
    val stepLenGt5 = sessionStatMap.getOrElse("step_length_gt_5", 0L)
    val sessionCount = sessionStatMap.getOrElse("session_count", 0L)
    //计算占比
    val visitLenLe10Ratio = Math.round(visitLenLe10 / sessionCount.toDouble * 10000) / 100.0
    val visitLenGt10Ratio = Math.round(visitLenGt10 / sessionCount.toDouble * 10000) / 100.0
    val stepLenLe5Ratio = Math.round(stepLenLe5 / sessionCount.toDouble * 10000) / 100.0
    val stepLenGt5Ratio = Math.round(stepLenGt5 / sessionCount.toDouble * 10000) / 100.0

    val sessionStat = SessionStat(taskId,conditions,sessionCount,visitLenLe10Ratio,visitLenGt10Ratio,stepLenLe5Ratio,stepLenGt5Ratio)
    sparkSession.sparkContext.makeRDD(Array(sessionStat))
  }
}
