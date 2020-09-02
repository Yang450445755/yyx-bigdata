package ruoze

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

    sc.parallelize(List(1,2,3,4,5),2)
      .mapPartitionsWithIndex((index, partition) => {
        println("这是一个分区")
        partition.map(x => s"分区:$index, 元素:$x")
      }).foreach(println)

//    sc.makeRDD(List("pk,pk,pk","ruoze,ruoze","xingxing"))
//        .flatMap(_.split(",")).foreach(println)
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

/*
    val a = sc.makeRDD(List(1,2,3,4,5,6),3)
    val b = sc.makeRDD(List(3,4,5,6,7,8,8),2)
    a.union(b).collect()
    a.union(b).partitions.size
    a.intersection(b).collect()
    a.subtract(b).collect()

    sc.makeRDD(List("拜仁慕尼黑","大巴黎")).cartesian(sc.makeRDD(List("胜","平","负"))).collect()

    sc.makeRDD(List("dog","tiger","lion","cat","owl")).keyBy(_.length).combineByKeyWithClassTag().collect()

    sc.makeRDD(List("a", "a", "a", "b", "b", "c")).groupBy(x => x).mapValues(_.size).collect()
    sc.makeRDD(1 to 9).groupBy(x=>{
      if(x%2==0) "even" else "odd"
    }).collect()

    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 80))).groupByKey().mapValues(_.sum).collect()
*/

    sc.stop()
  }

}
