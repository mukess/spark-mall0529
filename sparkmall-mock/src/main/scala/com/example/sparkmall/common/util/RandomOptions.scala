package com.example.sparkmall.common.util

import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomOptions {

  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    // 传入多个RanOpt对象，初始化选项池
    val randomOptions = new RandomOptions[T]()
    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight
      for (i <- 1 to opt.weight) {
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }


  def main(args: Array[String]): Unit = {
    val randomName = RandomOptions(RanOpt("zhangchen", 10), RanOpt("li4", 30))
    for (i <- 1 to 8) {
      println(i + ":" + randomName.getRandomOpt())
    }
  }


}

//随机选项
case class RanOpt[T](value: T, weight: Int) {
}

class RandomOptions[T](opts: RanOpt[T]*) {
  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt(): T = {
    //获得随机的对象value
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }
}

