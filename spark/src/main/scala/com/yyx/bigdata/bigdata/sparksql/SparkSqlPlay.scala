package com.yyx.bigdata.bigdata.sparksql

import java.util.Properties


import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Aaron-yang
  * @date 2020/9/13 20:36
  */
object SparkSqlPlay {

  /**
    * 0920讲
    * 1) text 我想输出几列就几列
    * 2) 根据TBL_NAME找到所有相关的信息
    * Garbage Collection Tuning
    * 连续登录天数(自己定3)  ==> RDD
    * pk,2020-09-20
    * pk,2020-09-20
    * pk,2020-09-21
    * pk,2020-09-22
    * pk,2020-09-24
    * pk,2020-09-25
    * ruoze,2020-09-20
    * ruoze,2020-09-22
    * ruoze,2020-09-24
    * ruoze,2020-09-25
    * @param args
    */


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("yyx")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    //读取本地文件
//    val df = spark.read.format("text").load("spark/access.log").show()

    //读取mysql
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "YANGyuxi1996")
    spark.read
      .jdbc("jdbc:mysql://yyxdata001:3306/erp_order", "image", properties)
      .show(false)











  }

}
