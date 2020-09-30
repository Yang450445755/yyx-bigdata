package com.yyx.bigdata.sparkstreaming.ruoze.ss01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SS编程模型：
 * 1) ssc
 * 2) DStream
 **/
object StreamingSocketWCApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName).setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // TODO...
//    val lines = ssc.socketTextStream("ruozedata001", 9527)  // Input DStreams
//    lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
//
//    val lines2 = ssc.socketTextStream("ruozedata001", 9527)  // Input DStreams
//    lines2.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()


    ssc.textFileStream("hdfs://ruozedata001:8020/ssdata").flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()

  }

}
