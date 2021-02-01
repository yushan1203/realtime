package com.atguigu.gmall2020.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

object OffsetManagerM {

  /**
    * 从Mysql中读取偏移量
    * @param groupId
    * @param topic
    * @return
    */
  def getOffset(topic:String,groupId:String):Map[TopicPartition,Long]= {
    val sql = "SELECT partition_id,topic_offset FROM offet_2020 WHERE topic='"+topic+"'AND group_id='"+groupId+"'"
    val partitionOffsetList:List[JSONObject] = MysqlUtil.queryList(sql)
    val topicPartitionMap:Map[TopicPartition,Long] = partitionOffsetList.map{jsonObj=>
      val topicPartition:TopicPartition = new TopicPartition(topic,jsonObj.getIntValue("partition_id"))
      val offset:Long = jsonObj.getLongValue("topic_offset")
      (topicPartition,offset)
    }.toMap
    topicPartitionMap
  }
}
