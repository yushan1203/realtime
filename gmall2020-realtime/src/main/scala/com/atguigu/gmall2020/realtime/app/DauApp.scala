package com.atguigu.gmall2020.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall2020.realtime.bean.DauInfo
import com.atguigu.gmall2020.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer



object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "GMALL_START"
    val groupId="DAO_GROUP"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputDstream:InputDStream[ConsumerRecord[String,String]] =null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputDstream= MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputDstream= MyKafkaUtil.getKafkaStream(topic,ssc)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //recordInputDstream.map(_.value()).print()

    val jsonObjDstream = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")

      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))

      jsonObj
    }
    val filteredDstream: DStream[JSONObject] = jsonObjDstream.transform{ rdd=>
      //println("过滤前：" +  rdd.count())
      val logInfoRdd:RDD[JSONObject]=rdd.mapPartitions{jsonObjItr=>
        //val jsonList: List[JSONObject] = jsonObjItr.toList//这条数据也影响es的插入
        val jedis:Jedis = RedisUtil.getJedisClient
        val filteredList = new ListBuffer[JSONObject]()
        for(jsonObj <- jsonObjItr){
          val dt: String = jsonObj.getString("dt")
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          val dauKey: String = "dau:"+dt
          val isNew: lang.Long = jedis.sadd(dauKey,mid)
          jedis.expire(dauKey,3600*24)//设置一个过期时间，24小时后失效
          if(isNew==1L){
            filteredList+=jsonObj
          }
        }
        jedis.close()
        filteredList.toIterator
      }
      //println("过滤后："+logInfoRdd.count())//这行代码打开后会导致下面的代码不能用，无法存到ES中，可能是因为print的缘故（行动算子的原因）
      logInfoRdd
    }

    //filteredDstream.print(1000)

    filteredDstream.foreachRDD{rdd=>
      rdd.foreachPartition{jsonItr=>

        if(offsetRanges!=null&&offsetRanges.size>0){
          val offsetRange: OffsetRange = offsetRanges(TaskContext.get().partitionId())
          println("from:"+offsetRange.fromOffset +" --- to:"+offsetRange.untilOffset)
        }

        val list: List[JSONObject] = jsonItr.toList
        //把源数据转换为要保存的数据
        val dauList: List[(String,DauInfo)] = list.map{ jsonObj=>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          val dauInfo = DauInfo(
            commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )
          dauInfo
          (dauInfo.mid,dauInfo)
        }
        dauList
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.bulkDoc(dauList,"gmall2020_dau_info_"+dt)

        OffsetManager.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
