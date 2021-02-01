package com.atguigu.gmall2020.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall2020.realtime.bean.dim.{BaseCategory3, SkuInfo}
import com.atguigu.gmall2020.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object SkuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_sku_info_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_SKU_INFO"
    val groupId = "sku_info_group"

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

    val objectDstream: DStream[SkuInfo] = inputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val obj: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
      obj
    }

    val skuInfoDstream: DStream[SkuInfo] = objectDstream.transform { rdd =>

      if (rdd.count() > 0) {
        val category3Sql = "select id,name from base_category3"
        val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
        val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        val tmSql = "select id,tm_name from base_trademark"
        val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
        val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        val spuSql = "select id,spu_name from spu_info"
        val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
        val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        val dimList: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](category3Map, tmMap, spuMap)
        val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

        val skuInfoRDD = rdd.mapPartitions { skuInfoItr =>
          val dimList: List[Map[String, JSONObject]] = dimBC.value
          val category3Map: Map[String, JSONObject] = dimList(0)
          val tmMap: Map[String, JSONObject] = dimList(1)
          val spuMap: Map[String, JSONObject] = dimList(2)

          val skuInfoList: List[SkuInfo] = skuInfoItr.toList
          for (skuInfo <- skuInfoList) {
            val category3JsonObj = category3Map.getOrElse(skuInfo.category3_id, null)
            if (category3JsonObj != null) {
              skuInfo.category3_name = category3JsonObj.getString("NAME")
            }
            val tmJsonObj = tmMap.getOrElse(skuInfo.tm_id, null)
            if (tmJsonObj != null) {
              skuInfo.tm_name = tmJsonObj.getString("TM_NAME")
            }
            val spuJsonObj = spuMap.getOrElse(skuInfo.spu_id, null)
            if (spuJsonObj != null) {
              skuInfo.spu_name = spuJsonObj.getString("SPU_NAME")
            }
          }

          skuInfoList.toIterator
        }
        skuInfoRDD
      } else {
        rdd
      }
    }
    skuInfoDstream.foreachRDD{rdd=>
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("SKU_INFO",Seq("ID","SPU_ID","PRICE","SKU_NAME","TM_ID","CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
