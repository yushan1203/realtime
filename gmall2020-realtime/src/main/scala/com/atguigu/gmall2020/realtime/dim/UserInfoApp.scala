package com.atguigu.gmall2020.realtime.dim

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.gmall2020.realtime.bean.dim.UserInfo
import com.atguigu.gmall2020.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_user_info_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_USER_INFO"
    val groupId = "user_info_group"

    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val userInfoDstream: DStream[UserInfo] = inputGetOffsetDstream.map { record =>
      val userInfoJsonStr: String = record.value()
      val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date = formattor.parse(userInfo.birthday)
      val cuTs: Long = System.currentTimeMillis()
      val betweenMs: Long = cuTs - date.getTime
      val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L
      if (age < 20) {
        userInfo.age_group = "20岁及以下"
      } else if (age > 30) {
        userInfo.age_group = "30岁以上"
      } else {
        userInfo.age_group = "21岁到30岁"
      }

      if (userInfo.gender == "M") {
        userInfo.gender_name = "男"
      } else {
        userInfo.gender_name = "女"
      }
      userInfo
    }
    userInfoDstream.foreachRDD{rdd=>
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("USER_INFO",Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME")
      ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
