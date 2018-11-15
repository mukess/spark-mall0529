package com.exapmle.sparkmall.realtime.module

import java.text.SimpleDateFormat
import java.util.Date

case class RealtimeAdsLog(date: Date, area: String, city: String, userId: String, adsId: String) {
  def toUserCountPerdayKey(): String = {
    val perDay: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    perDay + "_" + userId + "_" + adsId
  }

  def toAreaCityCountPerdayKey(): String = {
    val perDay: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    perDay + "_" + area + "_" + city + "_" + adsId
  }
}
