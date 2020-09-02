package com.yyx.bigdata

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aaron-yang
  * @date 2020/9/2 17:50
  */
object SparkRDDPlay {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("yyx-nb")
    val sc = new SparkContext(conf)


    sc.parallelize(List("yyxnb1","yyxnb2","yyxnb3")).foreach(println)


  }

}
