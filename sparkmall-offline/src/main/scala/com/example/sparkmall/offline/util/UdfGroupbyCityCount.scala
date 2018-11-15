package com.example.sparkmall.offline.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

class UdfGroupbyCityCount extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))

  override def bufferSchema: StructType = StructType(Array(StructField("countMap", MapType(StringType, LongType)), StructField("countSum", LongType)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HashMap[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName: String = input.getString(0)
    val countMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val countSum: Long = buffer.getAs[Long](1)
    buffer(0) = countMap + (cityName -> (countMap.getOrElse(cityName, 0L) + 1L))
    buffer(1) = countSum + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val countMap1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val countSum1: Long = buffer1.getAs[Long](1)
    val countMap2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val countSum2: Long = buffer2.getAs[Long](1)

    buffer1(0) = countMap1.foldLeft(countMap2) { case (map2, (cityName, count)) => map2 + (cityName -> (map2.getOrElse(cityName, 0L) + count)) }
    buffer1(1) = countSum1 + countSum2
  }

  override def evaluate(buffer: Row): Any = {
    val countMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val countSum: Long = buffer.getAs[Long](1)
    val cityRateList = new ListBuffer[CityRate]()
    for ((city, count) <- countMap) {
      val rate: Double = Math.round(count / countSum.toDouble * 1000) / 10.0
      cityRateList.append(CityRate(city, rate))
    }

    val cityRateSortedList: ListBuffer[CityRate] = cityRateList.sortWith { case (city1, city2) => city1.rate > city2.rate }
    val top2CityRate: ListBuffer[CityRate] = cityRateSortedList.take(2)

    var otherRate = 100.0
    if (cityRateSortedList.size > 2) {
      for (cityRate <- top2CityRate) {
        otherRate -= cityRate.rate
      }
      top2CityRate.append(CityRate("其他", otherRate))
    }
    top2CityRate.mkString(",")
  }


  case class CityRate(cityName: String, rate: Double) {
    override def toString: String = {
      cityName + ":" + rate + "%"
    }
  }

}
