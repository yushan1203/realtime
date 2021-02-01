package com.atguigu.gmall2020.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall2020.realtime.bean.OrderDetailWide
import com.atguigu.gmall2020.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("trademark_stat_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "DWS_ORDER_WIDE"
    val groupId = "trademark_stat_group"
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

    val jsonObjDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetailWide: OrderDetailWide = JSON.parseObject(jsonString,classOf[OrderDetailWide])
      orderDetailWide
    }
    jsonObjDstream.print(1000)

    val amountWithTmDstream: DStream[(String, Double)] = jsonObjDstream.map(orderWide=>(orderWide.tm_id+":"+orderWide.tm_name,orderWide.final_detail_amount))

    val amountByTmDstream: DStream[(String, Double)] = amountWithTmDstream.reduceByKey(_+_)

    amountByTmDstream.foreachRDD{rdd=>
      val amountArray: Array[(String, Double)] = rdd.collect()
      if(amountArray!=null&&amountArray.size>0){
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        DBs.setup()
        DB.localTx(implicit session=>{

          for ((tm,amount)<-amountArray){
            val tmArr: Array[String] = tm.split(":")
            val tmId = tmArr(0)
            val tmName = tmArr(1)
            val statTime: String = simpleDateFormat.format(new Date())
            println("数据写入执行")
            SQL("insert into trademark_amount_stat values(?,?,?,?)").bind(statTime,tmId,tmName,amount).update().apply()
          }
          for (offsetRange<-offsetRanges){
            val partitionId: Int = offsetRange.partition
            val untilOffset: Long = offsetRange.untilOffset
            println("偏移量提交执行")
            SQL("REPLACE INTO offset_2020(group_id,topic,partition_id,topic_offset) VALUES(?,?,?,?)")
              .bind(groupId,topic,partitionId,untilOffset).update().apply()
          }
        })
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
