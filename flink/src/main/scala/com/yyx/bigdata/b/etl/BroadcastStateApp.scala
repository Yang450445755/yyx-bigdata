package com.yyx.bigdata.etl

import java.sql.Connection
import java.util.Map

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author PK哥
 **/
object BroadcastStateApp {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(4)

      // 字段、不太变   00000001,ruozedata.com
      val stream1 = env.socketTextStream("ruozedata001", 9527)
        .map(x => {
          val splits = x.split(",")
          (splits(0).trim, splits(1).trim)
        })

      val mapState = new MapStateDescriptor[String,String]("map-state", classOf[String], classOf[String])
      val broadcastStream: BroadcastStream[(String, String)] = stream1.broadcast(mapState)

      // 日志数据
      val stream2 = env.socketTextStream("ruozedata001", 9528)
          .map(x => {
            val splits = x.split(",")
            val time = splits(0).trim
            val ip = splits(1).trim
            val domain = splits(2).trim
            (time, ip, domain)
          })

      val connectStream = stream2.connect(broadcastStream)
      connectStream.process(new BroadcastProcessFunction[(String,String,String),(String, String),(String,String,String,String)] {
        /**
         * 处理日志处理
         */
        override def processElement(value: (String, String, String), ctx: BroadcastProcessFunction[(String, String, String), (String, String), (String, String, String, String)]#ReadOnlyContext, out: Collector[(String, String, String, String)]): Unit = {

          val broadcastState = ctx.getBroadcastState(mapState)

          val time = value._1
          val ip = value._2
          val domain = value._3

          val userId = broadcastState.get(domain)
          out.collect((time, ip, domain, userId))
        }

        /**
         * 处理广播数据
         */
        override def processBroadcastElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String, String), (String, String), (String, String, String, String)]#Context, out: Collector[(String, String, String, String)]): Unit = {

          val userId = value._1
          val domain = value._2

          val broadcastState = ctx.getBroadcastState(mapState)
          broadcastState.put(domain, userId) // 把规则数据存储到state中

          val iterator = broadcastState.iterator()
          while(iterator.hasNext) {
            val next: Map.Entry[String, String] = iterator.next()

            println(next.getKey + "==>" + next.getValue + "==>" + getRuntimeContext.getIndexOfThisSubtask)
          }
        }
      })
          .print()

      env.execute(getClass.getCanonicalName)
    }
}
