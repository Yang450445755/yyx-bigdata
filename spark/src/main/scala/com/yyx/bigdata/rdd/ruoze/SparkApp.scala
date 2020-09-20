package com.yyx.bigdata.rdd.ruoze

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author PK哥
 **/
object SparkApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

//    sc.parallelize(List(List("a",1,3),List("a",2,4),List("b",1,1)))
//        .map(x => {
//          val key = x(0)  // 作为分组的key使用
//          val v1 = x(1).toString.toInt
//          val v2 = x(2).toString.toInt
//          (key, (v1,v2))
//        }).reduceByKey((x,y) => {
//      (x._1 + y._1, x._2+y._2)
//    }).foreach(println)

    sc.parallelize(List(
      "100000,一起看|电视剧|军旅|士兵突击,1,1",
      "100000,一起看|电视剧|军旅|士兵突击,1,0",
      "100001,一起看|电视剧|军旅|我的团长我的团,1,1"
    )).flatMap(x => {
      val splits = x.split(",")
      var id = splits(0)
      val nav = splits(1)
      val imp = splits(2).toInt
      val click = splits(3).toInt
      val navs = nav.split("\\|")
      navs.map(x => ((id,x),(imp, click)))
    }).reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).collect()
//        .groupByKey() //TODO...


    sc.stop()
  }
}
