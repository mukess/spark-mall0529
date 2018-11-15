package com.exapmle.sparkmall.realtime.app

import java.text.SimpleDateFormat

import com.exapmle.sparkmall.realtime.module.RealtimeAdsLog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object AdsCountPerHourApp {
  def adsCount(realtimeAdsLogStream: DStream[RealtimeAdsLog]): DStream[(String, String)] = {
    val lastHourAdsLogDStream: DStream[RealtimeAdsLog] = realtimeAdsLogStream.window(Minutes(60), Seconds(10))

    val lastHourAdsMinuCountDstream: DStream[(String, Long)] = lastHourAdsLogDStream.map { adslog =>
      val hourMinu: String = new SimpleDateFormat("HH:mm").format(adslog.date)
      (adslog.adsId + "_" + hourMinu, 1L)


    }.reduceByKey(_ + _)

    val lastHourMinuteCountPerAdsDstream: DStream[(String, Iterable[(String, Long)])] = lastHourAdsMinuCountDstream.map { case (adsMinuKey, count) =>
      val keyArr: Array[String] = adsMinuKey.split("_")
      val ads: String = keyArr(0)
      val hourMinu: String = keyArr(1)

      (ads, (hourMinu, count))
    }.groupByKey()

    val result: DStream[(String, String)] = lastHourMinuteCountPerAdsDstream.map { case (ads, minuItr) =>

      val hourminuCountJson = compact(render(minuItr))
      (ads, hourminuCountJson)

    }
    result

  }
}
