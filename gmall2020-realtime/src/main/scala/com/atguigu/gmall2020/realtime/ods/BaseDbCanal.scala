package com.atguigu.gmall2020.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object BaseDbCanal {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "GMALL2020_DB_C"
    val groupId="base_db_canal_group"
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

    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    jsonObjDstream.foreachRDD{rdd=>
      //推回kafka
      rdd.foreach{jsonObj=>
        val jsonArr = jsonObj.getJSONArray("data")
        val tableName: String = jsonObj.getString("table")
        val topic = "ODS_"+tableName.toUpperCase
        import scala.collection.JavaConversions._
        for(jsonObj<-jsonArr){
          println(jsonObj.toString)
          val msg: String = jsonObj.toString
          MyKafkaSink.send(topic,msg) //会重复，但是不会丢失，后面再做幂等
        }
      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
