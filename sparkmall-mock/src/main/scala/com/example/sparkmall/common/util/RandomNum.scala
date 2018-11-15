package com.example.sparkmall.common.util

import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割 canRepeat为false则不允许重复
    if (canRepeat) {
      val hitNums = new ListBuffer[Int]()
      while (hitNums.size < amount) {
        var randomNum = fromNum + new Random().nextInt(toNum - fromNum + 1)
        hitNums += randomNum
      }
      hitNums.mkString(delimiter)
    } else {
      val hitNums = new mutable.HashSet[Int]()
      while(hitNums.size < amount){
        var randomNum = fromNum + new Random().nextInt(toNum - fromNum + 1)
        hitNums += randomNum
      }
      hitNums.mkString(delimiter)
    }


  }

  def main(args: Array[String]): Unit = {
    multi(1,10,3,",",true).foreach(print)
  }

}

