package com.yyx.bigdata.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author Aaron-yang
  * @date 2020/9/13 20:36
  */
object SparkSqlPlay {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("yyx")
      .master("local[2]")
      .getOrCreate()


    spark.read.format("text").load("spark/access.log").show()

  }

}
