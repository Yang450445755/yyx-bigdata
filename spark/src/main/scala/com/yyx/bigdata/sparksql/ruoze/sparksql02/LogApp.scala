package com.yyx.bigdata.sparksql.ruoze.sparksql02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

/**
 * @author PK哥
 **/
object LogApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext.textFile("ruozedata-spark-sql/data/access.log")
      .map(x => {
        val splits = x.split("\t")
        val platform = splits(1)
        val traffic = splits(6).toLong
        val province = splits(8)
        val city = splits(9)
        val isp = splits(10)
        (platform, traffic, province, city, isp)
      }).toDF("platform", "traffic", "province", "city", "isp")



    //TODO... 基于platform分组，求每个province访问次数最多的TopN
    df.createOrReplaceTempView("log")
    spark.sql(
      """
        |
        |select a.* from
        |(
        | select t.*, row_number() over(partition by platform order by cnt desc) as r
        | from
        | (select platform,province, count(1) cnt from log group by platform,province) t
        |) a where a.r<=3
        |
        |""".stripMargin).show(100)

    df.groupBy("platform","province")
        .agg(count("*").as("cnt"))
        .select('platform,'province,'cnt,
          row_number().over(Window.partitionBy("platform").orderBy('cnt.desc)).as("r"))
        .filter("r <= 3").show()

//      df.groupBy("platform", "province", "city")
//          .agg(sum("traffic").as("traffics"))
//          .orderBy('traffics.desc)
//          .show()

//    df.createOrReplaceTempView("log")
//    spark.sql(
//      """
//        |
//        |select
//        |platform,province,city,sum(traffic) as traffics
//        |from
//        |log
//        |group by platform,province,city
//        |
//        |""".stripMargin).show()

    spark.stop()
  }
}
