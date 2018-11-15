package com.exapmle.sparkmall.realtime.app

import org.apache.spark.streaming.dstream.DStream

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object AreaTop3AdsCountPerDay {
  def adsCount(areaCityAdsCountDstream: DStream[(String, Long)]): DStream[(String, Map[String, String])] = {

    val areaAdsCountDstream: DStream[(String, Long)] = areaCityAdsCountDstream.map { case (dateAreaCityAdsKey, count) =>
      val keyArr: Array[String] = dateAreaCityAdsKey.split("_")
      val date: String = keyArr(0)
      val area: String = keyArr(1)
      val ads: String = keyArr(3)
      (date + "_" + area + "_" + ads, count)
    }.reduceByKey(_ + _)

    val areaTop3ItrPerDayDstream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCountDstream.map { case (cityAreaAds, count) =>
      val keyArr: Array[String] = cityAreaAds.split("_")
      val date: String = keyArr(0)
      val area: String = keyArr(1)
      val ads: String = keyArr(2)
      ("area_top3_ads" + date, (area, (ads, count)))

    }.groupByKey()

    val areaTop3AdsJsonPerDayDestream: DStream[(String, Map[String, String])] = areaTop3ItrPerDayDstream.map { case (dateKey, areaItr) =>
      val areaMap: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (ads, count)) => area }
      val areaTop3JsomMap: Map[String, String] = areaMap.map { case (area, areaAdsItr) =>
        val adsTop3List: List[(String, Long)] = areaAdsItr.map { case (area, (ads, count)) =>
          (ads, count)
        }.toList.sortWith { (adsCount1, adsCount2) => adsCount1._2 > adsCount2._2 }.take(3)
        //渲染+执行
        val top3AdsJson: String = compact(render(adsTop3List))
        (area, top3AdsJson)
      }
      (dateKey, areaTop3JsomMap)
    }
    areaTop3AdsJsonPerDayDestream

  }
}
