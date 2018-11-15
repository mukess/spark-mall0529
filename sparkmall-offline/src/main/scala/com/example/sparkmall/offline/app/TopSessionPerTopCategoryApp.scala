package com.example.sparkmall.offline.app

import com.example.sparkmall.model.UserVisitAction
import com.example.sparkmall.offline.resultmodel.{CatagoryTopN, TopSessionPerCid}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TopSessionPerTopCategoryApp {
  def getTopSession(sparkSession: SparkSession, taskId: String, topCategoryList: List[CatagoryTopN], userVisitActionRDD: RDD[UserVisitAction]): RDD[TopSessionPerCid] = {
    val topCategoryListBC: Broadcast[List[CatagoryTopN]] = sparkSession.sparkContext.broadcast(topCategoryList)
    val topCidActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>

      val topicidList: List[Long] = topCategoryListBC.value.map(catagoryTopN => catagoryTopN.category_id.toLong)
      topicidList.contains(userVisitAction.click_category_id)
    }
    val topCidSessionClickRDD: RDD[(String, Long)] = topCidActionRDD.map(action => (action.click_category_id + "_" + action.session_id, 1L))
    val topCidSessionCountRDD: RDD[(String, Long)] = topCidSessionClickRDD.reduceByKey(_ + _)

    val topSessionPerCidsRDD: RDD[(String, TopSessionPerCid)] = topCidSessionCountRDD.map { case (topcidSessionId, count) =>
      val topcidSessionIdArr: Array[String] = topcidSessionId.split("_")
      val topcid: String = topcidSessionIdArr(0)
      val sessionId: String = topcidSessionIdArr(1)
      (topcid, TopSessionPerCid(taskId, topcid, sessionId, count))
    }
    val topSessionItrPerCidsRDD: RDD[(String, Iterable[TopSessionPerCid])] = topSessionPerCidsRDD.groupByKey()

    val top10SessionPerCidRDD: RDD[TopSessionPerCid] = topSessionItrPerCidsRDD.flatMap { case (cid, topsessionItr) =>
      val top10sessionList: List[TopSessionPerCid] = topsessionItr.toList.sortWith { case (session1, session2) =>
        session1.clickCount > session2.clickCount
      }.take(10)
      top10sessionList
    }

    top10SessionPerCidRDD
  }

}
