package com.yyx.bigdata.etl.async

import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}

import scala.concurrent.{ExecutionContext, Future}

/**
 * 1) open: 创建Client
 * 2) close: 释放资源操作
 * 3) asyncInvoke: 完成业务逻辑
 *   resultFutureRequested: Future[OUT]
 *   resultFutureRequested.onSuccess中case的匹配类型与Future[OUT]一致
 *   匹配的业务逻辑中resultFuture.complete(Iterable(DataStream中真正的数据类型))
 * 4) 主方法中：AsyncDataStream(同步流, 自定义实现类, 超时时间, 时间的单位)
 **/
object HttpETLAsyncApp {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      // 2022-07-10 18:10:13,222.55.57.83,google.com,00000002
      // TODO... 读接口的数据
      val stream = env.socketTextStream("ruozedata001", 9527)
      val result = AsyncDataStream.unorderedWait(stream,
        new RuozedataAsyncHttpRequest, 1000, TimeUnit.MILLISECONDS)
      result.print()
      env.execute(getClass.getCanonicalName)
    }
}

class RuozedataAsyncHttpRequest extends RichAsyncFunction[String, AccessLogV2] {

  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  var httpClient:CloseableHttpAsyncClient = _

  override def open(parameters: Configuration): Unit = {
    val requestConfig: RequestConfig = RequestConfig.custom().setSocketTimeout(5000)
      .setConnectTimeout(5000)
      .build()


    httpClient = HttpAsyncClients.custom().setMaxConnTotal(20).setDefaultRequestConfig(requestConfig)
      .build()

    httpClient.start()
  }

  override def close(): Unit = {

    if(null != httpClient) httpClient.close()

  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[AccessLogV2]): Unit = {
    val splits = input.split(",")
    val time = splits(0)
    val ip = splits(1)
    val domain = splits(2)
    var province = "-"
    var city = "-"

    val url = s"https://restapi.amap.com/v3/ip?ip=$ip&output=json&key=${Keys.password}"

    try {
      val httpGet = new HttpGet(url)
      val future  = httpClient.execute(httpGet, null)

      val resultFutureRequested: Future[(String, String)] = Future {
        val response = future.get()
        val status = response.getStatusLine.getStatusCode
        val entity = response.getEntity
        if (status == 200) {
          val result = EntityUtils.toString(entity)
          val json = JSON.parseObject(result)
          province = json.getString("province")
          city = json.getString("city")
        }
        (province, city)
      }

      resultFutureRequested.onSuccess {
        case (province, city) => resultFuture.complete(Iterable(AccessLogV2(time, domain, province, city)))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
    }
  }
}
