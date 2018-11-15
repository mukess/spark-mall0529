package com.example.sparkmall.offline.app

import com.example.sparkmall.model.UserVisitAction
import com.example.sparkmall.offline.util.CategoryActionAccumulate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryCountApp {
  def categoryCount(sparkSession: SparkSession, userVisitAction: RDD[UserVisitAction]): mutable.HashMap[String, Long] = {

    val accumulate = new CategoryActionAccumulate

    sparkSession.sparkContext.register(accumulate)

    userVisitAction.foreach { action =>
      if (action.click_category_id > 0) {
        accumulate.add(action.click_category_id.toString + "_" + "click")
      } else if (null != action.order_category_ids) {
        val orderCids: Array[String] = action.order_category_ids.split(",")
        for (orderCId <- orderCids) {
          accumulate.add(orderCId + "_" + "order")
        }
      } else if (null != action.pay_category_ids) {
        val payCids: Array[String] = action.pay_category_ids.split(",")
        for (payCId <- payCids) {
          accumulate.add(payCId + "_" + "pay")
        }
      }
    }
    accumulate.value


  }

}
