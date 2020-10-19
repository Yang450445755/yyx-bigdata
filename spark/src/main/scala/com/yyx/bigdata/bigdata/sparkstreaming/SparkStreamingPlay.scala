package com.yyx.bigdata.bigdata.sparkstreaming

import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, storage}

/**
  * @author Aaron-yang
  * @date 2020/9/28 20:23
  */
object SparkStreamingPlay {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName("yyxnb")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //连接logstash
//    ssc.textFileStream("http://yyxdata001:9600")
    ssc.socketTextStream("yyxdata001",9527)
      .flatMap(_.split(","))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
