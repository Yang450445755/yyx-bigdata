package com.yyx.bigdata.bigdata.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aaron-yang
  * @date 2020/11/14 16:02
  */
object SparkRDDReview {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName("yyxnb")
    val sc = new SparkContext(sparkConf)

//    sc.parallelize(List(1,2,3,4,5)).map(_ * 5).foreach(println)

    val rdd = sc.textFile("hdfs://yyxdata001:9000/yyx/access.log")
      .map(_.split(",")(0))
      .map((_, 1))
      .reduceByKey(_ + _)

    rdd.cache()


    sc.stop()
  }
}
