package com.example.sparkmall.offline.resultmodel

case class SessionStat(taskId:String,conditions:String,session_count:Long,session_visitLength_le_10s_ratio:Double,
                       session_visitLength_gt_10s_ratio:Double,session_stepLength_le_5_ratio:Double,session_stepLength_gt_5_ratio:Double) {

}
