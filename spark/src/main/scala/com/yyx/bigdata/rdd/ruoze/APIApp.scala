package com.yyx.bigdata.rdd.ruoze

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * @author PK哥
 **/
object APIApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName(this.getClass.getSimpleName) // 生产上是不能这么写的 硬编码
    val sc = new SparkContext(sparkConf)

//    val rdd = sc.makeRDD(List(1 to 100 :_*),5)

    // 每一个元素就需要去创建一个connection  X
//    rdd.map(x => {
//      val connection = Random.nextInt(100)+""
//      println(connection)
//
//      val name = "ruoze_" + x  // ruoze_1 ....
//      // TODO... SQL操作
//
//      // TODO... connection关闭
//    }).collect()

//    rdd.mapPartitions(partition => {
//      val connection = Random.nextInt(100)+""
//      println(connection)
//      partition
//    }).collect()

//    rdd.foreach(x => {
//      val connection = Random.nextInt(100)+""
//            println(connection)
//
//            val name = "ruoze_" + x  // ruoze_1 ....
//            // TODO... SQL操作
//
//            // TODO... connection关闭
//    })

//    rdd.foreachPartition(partition => {
//      val connection = Random.nextInt(100)+""
//      println(connection)
//    })


    val rdd = sc.makeRDD(List(1 to 6: _*), 3)
    println(rdd.partitions.size)
    rdd.mapPartitionsWithIndex((index, partition) => {
      partition.map(x => s"分区:$index, 元素:$x")
    }).collect()

//    val rdd2 = rdd.coalesce(2)
//    println(rdd2.partitions.size)
//    rdd2.mapPartitionsWithIndex((index, partition) => {
//      partition.map(x => s"分区:$index, 元素:$x")
//    }).collect()
//
//    val rdd3 = rdd.coalesce(6)

    val rdd2 =rdd.repartition(4)
    println(rdd2.partitions.size)
        rdd2.mapPartitionsWithIndex((index, partition) => {
          partition.map(x => s"分区:$index, 元素:$x")
        }).collect()

    sc.stop()
  }
}
