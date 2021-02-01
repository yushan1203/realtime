package com.atguigu.gmall2020.realtime.util


import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core
import io.searchbox.core.{Bulk, BulkResult, Index, Search}


object MyEsUtil {

  var factory: JestClientFactory =null

  def getClient:JestClient = {
    if(factory==null)build();
    factory.getObject
  }

  def build():Unit = {
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
     .maxTotalConnection(20)
     .connTimeout(10000).readTimeout(10000).build())
  }

  def addDoc():Unit = {
    val jest:JestClient = getClient
    val index = new Index.Builder(Movie("4","龙岭迷窟","鬼吹灯")).index("movie_test_index").`type`("_doc").id("4").build()
    val message:String = jest.execute(index).getErrorMessage
    if(message!=null){
      println(message)
    }
    jest.close()
  }

  def main(args: Array[String]): Unit = {
    addDoc()
  }

  def bulkDoc(sourceList:List[(String,Any)],indexName:String):Unit={
      if(sourceList!=null&&sourceList.size>0){
        val jest:JestClient = getClient
        val bulkBuilder = new Bulk.Builder
        for((id,source)<-sourceList){
          val index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()

          bulkBuilder.addAction(index)
        }
        val bulk:Bulk = bulkBuilder.build()
        val result:BulkResult = jest.execute(bulk)
        val items: util.List[BulkResult#BulkResultItem] = result.getItems
        println("保存到ES："+items.size()+"条数据")
        jest.close()
      }

    }

  def queryDoc():Unit = {
    val jest:JestClient = getClient
    val query = "{\n" +
      "  \"query\": {\n" +
      "    \"match\": {\n" +
      "      \"actorList.name\": \"张译\"\n" +
      "    }\n" +
      "  }\n" +
      "}";
    val search = new Search.Builder(query).addIndex("movie_index2020").addType("_doc").build()
    val result: core.SearchResult = jest.execute(search)
    val hits: util.List[core.SearchResult#Hit[Map[String, Any], Void]] = result.getHits(classOf[Map[String,Any]])
    import scala.collection.JavaConversions._
   for(hit<-hits){
     println(hit.source.mkString(","))
   }
    jest.close()
  }



  case class Movie(id:String,movie_name:String,name:String);
}
