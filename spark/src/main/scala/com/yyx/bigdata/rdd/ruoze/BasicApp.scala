package com.yyx.bigdata.rdd.ruoze

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author PK哥
 **/
object BasicApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //.set("spark.master","local").set("spark.app.name",this.getClass.getSimpleName)
      .setMaster("local").setAppName(this.getClass.getSimpleName) // 生产上是不能这么写的 硬编码
    val sc = new SparkContext(sparkConf)

//    sc.parallelize(List(1,2,3,4,5),2)
//      .mapPartitionsWithIndex((index, partition) => {
//        println("这是一个分区")
//        partition.map(x => s"分区:$index, 元素:$x")
//      }).foreach(println)

    sc.makeRDD(List("pk,pk,pk","ruoze,ruoze","xingxing"))
        .flatMap(_.split(",")).foreach(println)
//
//    val rdd = sc.parallelize(List(1,2,3,4,5))
//    rdd.filter(_ > 2).collect()
//    rdd.filter(_ % 2 ==0).filter(_ > 2).collect()
//    rdd.filter(x => x % 2 ==0 &&  x > 2).collect()
//
//    sc.parallelize(1 to 20, 4).glom().collect()
//
//    sc.makeRDD(1 to 20).sample(false, 0.2).collect()
//
//    val a = sc.makeRDD(List("ruoze","pk","xingxing","jepson"))
//    val b = sc.makeRDD(List(30,31,60))
//    a.zip(b).collect()
//
//    sc.makeRDD(List(("ruoze",30),("jepson",18))).mapValues(_+2).collect()
//
//    sc.makeRDD(List("dog","tiger","lion","cat","owl"))
//        .map(x => (x.length, x))
//        .flatMapValues("-"+_+"=").collect()
//
//    // join: select e.ename,d.dname  from emp e join dept d on e.deptno = d.deptno
//    val left = sc.makeRDD(List(("若泽","北京"),("J总","上海"),("PK","深圳")))
//    val right = sc.makeRDD(List(("若泽",30),("J总",18),("星星",80)))
//    left.join(right)
//    left.cogroup(right).collect()

    val a = sc.makeRDD(List(1,2,3,4,5,6),3)
    val b = sc.makeRDD(List(3,4,5,6,7,8,8),2)
    a.union(b).collect()
    a.union(b).partitions.size
    a.intersection(b).collect()
    a.subtract(b).collect()

    sc.makeRDD(List("拜仁慕尼黑","大巴黎")).cartesian(sc.makeRDD(List("胜","平","负"))).collect()

    sc.makeRDD(List("dog","tiger","lion","cat","owl")).keyBy(_.length).collect()

    sc.makeRDD(List("a", "a", "a", "b", "b", "c")).groupBy(x => x).mapValues(_.size).collect()
    sc.makeRDD(1 to 9).groupBy(x=>{
      if(x%2==0) "even" else "odd"
    }).collect()

    // (String, Iterable[Int])
    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 80))).groupByKey().mapValues(_.sum).collect()

    // (a,<1,80>) (b,<2>) (c,<3>)  ==> (String, Int)
    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 80))).reduceByKey((x,y)=>x+y).collect()

    val rdd = sc.makeRDD(List(("若泽",30),("PK",31),("星星",16)))
    rdd.sortBy(-_._2).collect()

    rdd.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).collect()

    sc.makeRDD(List(3,4,5,6,7,8,8)).distinct().collect()

    /**
     * TODO...
     * 1) 8 ==> (8,null)   8 ==> (8,null)
     * 2) reduceByKey((a,b)=>a)
     */
    sc.makeRDD(List(3, 4, 5, 6, 7, 8, 8)).map(x => (x, null)).reduceByKey((a, b) => a).map(_._1).collect()


    // 链式编程


    sc.makeRDD(List("若泽","PK","星星")).zipWithIndex().collectAsMap()

    sc.makeRDD(List(3, 4, 5, 6, 7, 8, 8)).takeOrdered(2)(Ordering.by(x => -x))
    sc.makeRDD(List(3, 4, 5, 6, 7, 8, 8)).top(3)


    sc.makeRDD(1 to 100).takeSample(true, 10)

    sc.makeRDD(List(1,2,3,4),3).foreachPartition(partition => {
      println("这是一个分区")
      for(ele <- partition) {
        println(ele)
      }
    })


    sc.makeRDD(List("若泽","若泽", "PK","星星")).countByValue()

    sc.makeRDD(List(("a","1"),("a","2"),("b","1"))).lookup("a")
    sc.makeRDD(1 to 100).fold(0)(_+_)
    sc.stop()
  }

}
