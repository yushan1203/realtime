package com.atguigu.gmall2020.realtime.dws

import java.util.Properties
import java.{lang, util}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall2020.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.atguigu.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("order_wide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicOrderInfo = "DWD_ORDER_INFO"
    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdOrderInfo = "dws_order_info_group"
    val groupIdOrderDetail = "dws_order_detail_group"


    val orderInfoKafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupIdOrderInfo)
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoKafkaOffsetMap != null && orderInfoKafkaOffsetMap.size > 0) {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, orderInfoKafkaOffsetMap, groupIdOrderInfo)
    } else {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupIdOrderInfo)
    }

    val orderDetailKafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupIdOrderDetail)
    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailKafkaOffsetMap != null && orderDetailKafkaOffsetMap.size > 0) {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, orderDetailKafkaOffsetMap, groupIdOrderDetail)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupIdOrderDetail)
    }

    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo
    }
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }

    //orderInfoDstream.print(1000)
    //orderDetailDstream.print(1000)

    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
//    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)
//    orderJoinedDstream.print(1000)

    val orderInfoWithKeyWindowDstream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDstream.window(Seconds(100),Seconds(5))
    val orderDetailWithKeyWindowDstream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDstream.window(Seconds(100),Seconds(5))

    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowDstream.join(orderDetailWithKeyWindowDstream)

    val orderJoinedNewDstream = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "order_join_keys"
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]()
      for ((orderId, (orderInfo, orderDetail)) <- orderJoinedTupleItr) {
        val ifNew: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderInfo, orderDetail)))
        }
      }
      jedis.close()
      orderJoinedNewList.toIterator
    }

    val orderDetailWideDstream: DStream[OrderDetailWide] = orderJoinedNewDstream.map{case(orderId,(orderInfo,orderDetail))=>new OrderDetailWide(orderInfo,orderDetail)}

    //orderDetailWideDstream.print(1000)

    val orderWideWithSplitDstream: DStream[OrderDetailWide] = orderDetailWideDstream.mapPartitions { orderWideItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      for (orderWide <- orderWideList) {
        val key = "order_split_amount:" + orderWide.order_id
        val orderSumMap: util.Map[String, String] = jedis.hgetAll(key)
        var splitAmountSum = 0D
        var originAmountSum = 0D
        if (orderSumMap != null && orderSumMap.size() > 0) {
          val splitAmountSumString: String = orderSumMap.get("split_amount_sum")
          splitAmountSum = splitAmountSumString.toDouble
          val originAmountSumString: String = orderSumMap.get("origin_amount_sum")
          originAmountSum = originAmountSumString.toDouble
        }

        val detailOriginAmount: Double = orderWide.sku_num * orderWide.sku_price
        val restOriginAmount: Double = orderWide.final_total_amount - originAmountSum
        if (detailOriginAmount == restOriginAmount) {
          orderWide.final_detail_amount = orderWide.final_total_amount - splitAmountSum
        } else {
          orderWide.final_detail_amount = detailOriginAmount * orderWide.final_total_amount / orderWide.original_total_amount
          orderWide.final_detail_amount = Math.round(orderWide.final_detail_amount*100D)/100D
        }
        splitAmountSum += orderWide.final_detail_amount
        originAmountSum += detailOriginAmount
        orderSumMap.put("split_amount_sum", splitAmountSum.toString)
        orderSumMap.put("origin_amount_sum", originAmountSum.toString)
        jedis.hmset(key, orderSumMap)
      }

      jedis.close()
      orderWideList.toIterator
    }

    //orderWideWithSplitDstream.cache()
    //orderWideWithSplitDstream.map(orderwide=>JSON.toJSONString(orderwide,new SerializeConfig(true))).print(1000)

    val sparkSession = SparkSession.builder()
        .appName("order_detail_wide_spark_app")
        .getOrCreate()

    import sparkSession.implicits._

    val orderWideKafkaSendDstream: DStream[OrderDetailWide] = orderWideWithSplitDstream.mapPartitions { orderWideItr =>
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      for (orderWide <- orderWideList) {
        MyKafkaSink.send("DWS_ORDER_WIDE", JSON.toJSONString(orderWide, new SerializeConfig(true)))
      }
      orderWideList.toIterator
    }
    orderWideKafkaSendDstream

    orderWideKafkaSendDstream.foreachRDD{rdd=>
      val df = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize","100")
        .option("isolationLevel","NONE")
        .option("numPartitions","4")
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hadoop102:8123/default","order_wide",new Properties())

      OffsetManager.saveOffset(topicOrderInfo,groupIdOrderInfo,orderInfoOffsetRanges)
      OffsetManager.saveOffset(topicOrderDetail,groupIdOrderDetail,orderDetailOffsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
  }
}
