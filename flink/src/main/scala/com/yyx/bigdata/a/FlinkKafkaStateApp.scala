package com.yyx.bigdata.a

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author PK哥
 **/
object FlinkKafkaStateApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val brokers = "ruozedata001:9092,ruozedata001:9093,ruozedata001:9094"
    val topic = "flink9876"


    // 容错
    env.enableCheckpointing(5000)
    val beckend:StateBackend = new FsStateBackend("file:///D:/workspaces/ruozedata-flink/state")
    env.setStateBackend(beckend)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(5, TimeUnit.SECONDS)))


    // Kafka参数相关的
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", "ruozedata-flink-kafka-111")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false")
    val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    kafkaSource.setCommitOffsetsOnCheckpoints(false) // default=true
    val stream = env.addSource(kafkaSource)

    /**
     * 1) 不带savepoint  默认是特殊的topic的offset上进行消费
     * 2) 带了savepoint  就是以savepoint所指定的路径的元数据进行消费
     */

    stream.flatMap(_.split(","))
      .map((_,1))
      .keyBy(x=>x._1)
      .sum(1)
      .print()

    env.socketTextStream("ruozedata001", 9527)
      .map(x => {
        if(x.contains("pk")) {
          throw  new RuntimeException("PK哥来了，快跑...")
        } else {
          x.toLowerCase()
        }
      }).print()


    env.execute("若泽数据Flink...")
  }

}
