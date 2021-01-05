package com.yyx.bigdata.etl.db

import java.sql.Connection

import com.ruozedata.flink.bean.Domain.AccessLog
import com.ruozedata.flink.utils.MySQLUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * @author PK哥
 **/
object UserDomainMappingApp {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      // 2022-07-10 18:10:13,222.55.57.83,google.com,00000002
      // TODO... 读MySQL的数据
      val stream = env.socketTextStream("ruozedata001", 9527)
      stream.map(new RichMapFunction[String, AccessLog] {
        var connection:Connection = _
        override def open(parameters: Configuration): Unit = {
          connection = MySQLUtils.getConnection()
        }

        override def close(): Unit = {
          MySQLUtils.closeConnection(connection)
        }

        override def map(value: String): AccessLog = {
          val splits = value.split(",")
          val time = splits(0)
          val domain = splits(2)
          var userId = "-"

          val pstmt = connection.prepareStatement("select user_id from user_domain_mapping where domain=?")
          pstmt.setString(1, domain)
          val rs = pstmt.executeQuery()

          if(rs.next()) {
            userId = rs.getString(1)
          }

          AccessLog(domain, userId, time)
        }
      }).print()

      env.execute(getClass.getCanonicalName)
    }
}
