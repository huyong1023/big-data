package com.huyong.bigdata.spark.util

import java.util.Objects
import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


/**
  * Created by yonghu on 2020/4/23.
  */
object MyEsUtil {

  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null


  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
        .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build)
  }

  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
    val source = "{}"
    val index: Index = new Index.Builder(source).index("").`type`("_doc").build()
    jest.execute(index)
    close(jest)

  }


  def indexBulk(indexName: String, list: List[Any]): Unit ={
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for (doc <- list ) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }

    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
    println("")
    close(jest)

  }
}
