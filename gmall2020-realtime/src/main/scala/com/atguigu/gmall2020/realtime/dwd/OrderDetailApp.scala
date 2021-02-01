package com.atguigu.gmall2020.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall2020.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("order_detail_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_ORDER_DETAIL"
    val groupId = "order_detail_group"
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

    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions { orderDetailItr =>
      val orderDetailList: List[OrderDetail] = orderDetailItr.toList
      if (orderDetailList.size > 0) {
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        val sql = "select id,tm_id,spu_id,category3_id,tm_name,spu_name,category3_name from sku_info where id in ('" + skuIdList.mkString("','") + "')"
        val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val skuJsonObjMap = skuJsonObjList.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
        for (orderDetail <- orderDetailList) {
          val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
          orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
          orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
          orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
          orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
          orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
          orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
        }
      }
      orderDetailList.toIterator
    }
    orderDetailWithSkuDstream

    orderDetailWithSkuDstream.foreachRDD{rdd=>
      rdd.foreach{orderDetail=>
        val orderDetailJsonString:String = JSON.toJSONString(orderDetail,new SerializeConfig(true))
          MyKafkaSink.send("DWD_ORDER_DETAIL",orderDetail.order_id.toString,orderDetailJsonString)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
