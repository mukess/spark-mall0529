package com.exapmle.sparkmall.realtime.app

import com.exapmle.sparkmall.realtime.module.RealtimeAdsLog
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

object AreaCityAdsCountPerDayApp {
  def adsCount(realtimeDStream: DStream[RealtimeAdsLog]): DStream[(String, Long)] = {
    val areaCityClickDStream: DStream[(String, Long)] = realtimeDStream.map(realtimelog => (realtimelog.toAreaCityCountPerdayKey(), 1L))
    val areaCityCountDStream: DStream[(String, Long)] = areaCityClickDStream.reduceByKey(_ + _)

    //此处有问题，如何截取当天的访问次数；values是当次总共的访问量，sumOption是历史数据
    val areaCityTotalCountDStream: DStream[(String, Long)] = areaCityCountDStream.updateStateByKey { (values: Seq[Long], sumOption: Option[Long]) =>

      println(s"values.sum = ${values.mkString("||")}")
      //得到本次的sum
      val thisSum: Long = values.sum
      var sumValue: Long = sumOption.getOrElse(0L)
      if (values.sum != null) {
        sumValue += thisSum
      }
      //此处确定一定会有值，所以返回some()
      Some(sumValue)
    }
    areaCityTotalCountDStream

  }

}
