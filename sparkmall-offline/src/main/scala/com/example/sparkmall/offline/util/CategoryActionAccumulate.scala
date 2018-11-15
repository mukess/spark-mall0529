package com.example.sparkmall.offline.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryActionAccumulate extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var sessionStatMap = new mutable.HashMap[String, Long]()


  override def isZero: Boolean = {
    sessionStatMap.size == 0
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryActionAccumulate()
  }

  override def reset(): Unit = {
    sessionStatMap = new mutable.HashMap[String, Long]()
  }

  override def add(key: String): Unit = {
    sessionStatMap(key) = sessionStatMap.getOrElse(key, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    other match {
      case sessionStatAccumulate: CategoryActionAccumulate =>
        sessionStatAccumulate.sessionStatMap.foldLeft(sessionStatMap) {
          case (sMap, (key, count)) =>
            sMap(key) = sMap.getOrElse(key, 0L) + count
            sMap
        }
    }
  }


  override def value: mutable.HashMap[String, Long] = {
    sessionStatMap
  }


}
