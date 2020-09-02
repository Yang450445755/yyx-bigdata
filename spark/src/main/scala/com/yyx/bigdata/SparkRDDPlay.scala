package com.yyx.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aaron-yang
  * @date 2020/9/1 22:24
  */
object SparkRDDPlay {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(sparkConf)

//    sc.parallelize(List("yyx","dwq"))..foreach(println)


  }

}
