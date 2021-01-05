package com.yyx.bigdata.etl.async

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util
import java.util.concurrent.{Callable, ExecutorService, TimeUnit}

import com.alibaba.druid.pool.DruidDataSource
import com.ruozedata.flink.bean.Domain.AccessLog
import com.ruozedata.flink.utils.MySQLUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.duration.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

/**
 * @author PK哥
 **/
object UserDomainMappingAsyncApp {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      // 2022-07-10 18:10:13,222.55.57.83,google.com,00000002
      val stream = env.socketTextStream("ruozedata001", 9527)

      val result  = AsyncDataStream.unorderedWait(stream, new AsyncMySQLRequest, 1000, TimeUnit.MILLISECONDS)
      result.print()

      env.execute(getClass.getCanonicalName)
    }
}


class AsyncMySQLRequest extends RichAsyncFunction[String, String] {


  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  var executorService: ExecutorService = null
  var dataSource: DruidDataSource = null

  override def open(parameters: Configuration): Unit = {
    executorService = util.concurrent.Executors.newFixedThreadPool(20)

    dataSource = new DruidDataSource()
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    dataSource.setUsername("root")
    dataSource.setPassword("!Ruozedata123")
    dataSource.setUrl("jdbc:mysql://ruozedata001:3306/ruozedata")
    dataSource.setInitialSize(5)
    dataSource.setMinIdle(10)
    dataSource.setMaxActive(20)
  }

  override def close(): Unit = {
    if(null != dataSource) dataSource.close()
    if(null != executorService) executorService.shutdown()
  }

  // TODO...
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    val future: util.concurrent.Future[String] = executorService.submit(new Callable[String] {
      override def call(): String = query(input)
    })

    val resultFutureRequested: Future[String]  =  Future{
      future.get()
    }

    resultFutureRequested.onSuccess {
      case result: String => resultFuture.complete(Iterable(result))
    }
  }

  def query(domain:String) = {
    var connection:Connection = null
    var pstmt:PreparedStatement = null
    var rs:ResultSet = null
    val sql = "select user_id from user_domain_mapping where domain=?"
    var result = "-"
    try{
      connection = dataSource.getConnection
      pstmt = connection.prepareStatement(sql)
      pstmt.setString(1, domain)
      rs = pstmt.executeQuery()
      if(rs.next()) {
        result = rs.getString(1)
      }

    } finally {
      if(null!=rs) rs.close()
      if(null!=pstmt) pstmt.close()
      if(null!=connection) connection.close()
    }
    result
  }
}