package com.example.sparkmall.offline.app

import com.example.sparkmall.offline.resultmodel.SessionInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SessionRandomExtractor {
  val needSessionNum = 1000

  def extractSessions(sparkSession: SparkSession, sessionInfoRDD: RDD[(String, SessionInfo)]): RDD[SessionInfo] = {
    val sessionCount: Long = sessionInfoRDD.count()

    val dayHourSessionRDD = sessionInfoRDD.map { case (sessionId, sessionInfo) =>
      val dateHourKey = sessionInfo.startTime.split(":")(0)
      (dateHourKey, sessionInfo)


    }

    val dayHourSessionsItrRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionRDD.groupByKey()

    dayHourSessionsItrRDD.flatMap { case (dayHour, sessionsItr) =>
      val dayhourSessionCount: Int = sessionsItr.size
      val dayhourNeedSessionNum = dayhourSessionCount.toDouble / sessionCount * needSessionNum
      val sessionList = randomExtract(sessionsItr.toArray, dayhourNeedSessionNum.toLong)
      sessionList
    }


  }

  def randomExtract[T](array: Array[T], num: Long): List[T] = {
    //
    val hitIdxSet = new mutable.HashSet[Int]()
    val hitValue = new ListBuffer[T]

    while (hitValue.size < num) {
      val ranIdx = new Random().nextInt(array.size)
      if (!hitIdxSet.contains(ranIdx)) {
        val value = array(ranIdx)
        hitValue += value
      }
    }
    hitValue.toList

  }

}
