package com.yyx.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
  * @author Aaron-yang
  * @date 2020/9/1 22:24
  */
object SparkRDDPlay {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(sparkConf)

//    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 80))).reduceByKey((x,y)=>x+y).collect()

//    sc.parallelize(List("yyx","dwq")).foreach(println)


    /**
      * 1363157985066,120.196.100.82,1,2
      * 1363157995052,120.197.40.4,1,2
      */
/*    val map = sc.textFile("spark/access.log")
      .flatMap(s => {

        val strings = s.split(",")
        val tel = strings(0)
        val ip = strings(1)
        val uv = strings(2).toInt
        val pv = strings(3).toInt

        Map(
          tel -> pv
        )
      })
    map.reduceByKey((x,y) => x+y).foreach(println)
    println("---------------------------------------")
    map.groupByKey().map(s=>(s._1,s._2.sum)).foreach(println)*/


/*    val mapRdd = sc.parallelize(List(
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
      navs.map(x => ((id, x), (imp, click)))
    })
    mapRdd.reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).foreach(println)

    /**
      * ((100000,一起看),(2,1))
      * ((100001,电视剧),(1,1))
      * ((100001,军旅),(1,1))
      * ((100001,一起看),(1,1))
      * ((100001,我的团长我的团),(1,1))
      * ((100000,军旅),(2,1))
      * ((100000,士兵突击),(2,1))
      * ((100000,电视剧),(2,1))
      */
    mapRdd.groupByKey().map(a => {
      var key  = 0
      var value = 0
      a._2.foreach(b=>{
        key = key+b._1
        value = value+b._2
      })
      (a._1, (key,value))
    }
    ).foreach(println)*/


    sc.textFile("hdfs://yyxdata001:9000/yyx/mockdata.txt")






  }

}
