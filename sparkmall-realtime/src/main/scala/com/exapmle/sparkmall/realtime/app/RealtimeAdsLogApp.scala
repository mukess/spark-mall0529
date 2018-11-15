package com.exapmle.sparkmall.realtime.app

import java.util
import java.util.Date

import com.example.sparkmall.common.util.{ConfigurationUtil, KafkaUtil, RedisUtil}
import com.exapmle.sparkmall.realtime.module.RealtimeAdsLog
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


object RealtimeAdsLogApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("realtime_log").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setCheckpointDir("./checkpoint")

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log", ssc)

    val realtimeLogLineDStream: DStream[String] = inputDstream.map(recorder => recorder.value())

    val realtimeLogDstream: DStream[RealtimeAdsLog] = realtimeLogLineDStream.map { line =>
      val logArray: Array[String] = line.split(" ")
      val milsec: Long = logArray(0).toLong
      RealtimeAdsLog(new Date(milsec), logArray(1), logArray(2), logArray(3), logArray(4))
    }

    realtimeLogDstream.cache()

    val jedis: Jedis = RedisUtil.getJedisClient
    //此处不理解transform和foreachRdd/foreachPartition区别：transform可以支持算子操作之前的操作在driver端运行
    val realtimeFilteredLogDstream: DStream[RealtimeAdsLog] = realtimeLogDstream.transform { rdd =>
      val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
      //此处能否用RedisUtil
      val redishost: String = config.getString("redis.host")
      val redisPort = config.getString("redis.port")
      val jedis = new Jedis(redishost, redisPort.toInt)
      val blackList: util.Set[String] = jedis.smembers("blackList")
      jedis.close()

      val blackListBC: Broadcast[util.Set[String]] = sparkSession.sparkContext.broadcast(blackList)

      rdd.filter { realtimeLog =>
        !blackListBC.value.contains(realtimeLog.userId)
      }
    }
    //foreachRdd/foreachPartition区别:每个rdd中都有partitipon
    realtimeFilteredLogDstream.foreachRDD { rdd =>
      rdd.foreachPartition { realtimeLogItr =>
        //此处创建连接池是为了在每个分区中都能使用一个jedis，节省性能
        val jedisClient: Jedis = RedisUtil.getJedisClient
        realtimeLogItr.foreach { realtimeLog =>
          jedisClient.hincrBy("user_count_ads_perday", realtimeLog.toUserCountPerdayKey(), 1L)
          val count: String = jedisClient.hget("user_count_ads_perday", realtimeLog.toUserCountPerdayKey())
          if (count.toInt >= 100) {
            jedisClient.sadd("blackList", realtimeLog.userId)
          }
        }
        jedisClient.close()

      }
    }

    //需求八
    val areaCityTotalCountDStream: DStream[(String, Long)] = AreaCityAdsCountPerDayApp.adsCount(realtimeFilteredLogDstream)
    areaCityTotalCountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { areaCityTotalCountItr =>
        val jedisClient: Jedis = RedisUtil.getJedisClient
        for ((areaCityTotalCount, count) <- areaCityTotalCountItr) {
          jedisClient.hset("area_city_ads_count", areaCityTotalCount, count.toString)
        }
        jedisClient.close()

      }

    }


    //需求九 redis-cli --raw
    AreaTop3AdsCountPerDay.adsCount(areaCityTotalCountDStream).foreachRDD(
      rdd => {
        val areaAdsCountPerDayArr: Array[(String, Map[String, String])] = rdd.collect()

        for ((dataKey, areaTop3AdsMap) <- areaAdsCountPerDayArr) {
          import scala.collection.JavaConversions._

          jedis.hmset(dataKey, areaTop3AdsMap)
        }
      }
    )

    //需求十
    val lastHourMinuCountJsonPerAdsDstream: DStream[(String, String)] = AdsCountPerHourApp.adsCount(realtimeLogDstream)
    lastHourMinuCountJsonPerAdsDstream.foreachRDD { rdd =>
      val lastHourMap: Map[String, String] = rdd.collect().toMap

      import scala.collection.JavaConversions._
      jedis.del("last_hour_ads_click")
      jedis.hmset("last_hour_ads_click", lastHourMap)
    }

    ssc.start()
    ssc.awaitTermination()
    jedis.close()
  }

}
