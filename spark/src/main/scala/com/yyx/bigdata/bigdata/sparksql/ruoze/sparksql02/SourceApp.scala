package com.yyx.bigdata.bigdata.sparksql.ruoze.sparksql02

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * @author Aaron-yang
  * @date 2020/9/19 21:33
  */
object SourceApp {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._





//    val df = spark.read.format("jdbc")
//        .option("url","jdbc:mysql://ruozedata001:3306")
//        .option("dbtable","ruozedata.emp")
//      .option("user", "root")
//      .option("password", "!Ruozedata123")
//      .load()
//    df.filter('DEPTNO === 20)
//        .write.format("jdbc")
//      .option("url","jdbc:mysql://ruozedata001:3306")
//      .option("dbtable","ruozedata.emp_01")
//      .option("user", "root")
//      .option("password", "!Ruozedata123")
//        .save()

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "!Ruozedata123")
    spark.read
      .jdbc("jdbc:mysql://ruozedata001:3306", "ruozedata.emp", properties)
        .orderBy('SAL.desc).show()
    spark.stop()
  }

  def json(spark:SparkSession): Unit = {
    //    val df = spark.read.format("json").load("ruozedata-spark-sql/data/access.json")
    //df.printSchema()
    //    df.show(false)
    //    df.select("appId","platform","traffic")
    //      .filter('user === "ruozedata73")
    //        .show(false)

    // TODO...  读进来df是所有的字段数据，只需要提取其中的某几个字段
    //    df.select("appId","user")
    //      .where('user === "ruozedata73")
    //        .write.format("json").mode(SaveMode.Append).save("out")

//    val df = spark.read.format("json").load("ruozedata-spark-sql/data/people2.json")
    //    df.printSchema()
    //    df.show()

//    df.select('name, 'age, $"info.work", $"info.home").show()

//===============================================================

//    val df = spark.read.format("text").load("ruozedata-spark-sql/data/people.txt")
//    //    df.printSchema()
//
//    // text : 仅支持1列，而且还不支持int类型
//    df.map(x => {
//      val splits = x.getString(0).split(",")
//      (splits(0),splits(1).trim.toInt)
//    }).toDF("name","age")
//      .select("name")
//      .write
//      .option("compression","lzo")
//      .format("text").mode(SaveMode.Overwrite)
//      .save("out")

    //===============================================================

//    spark.read.format("csv")
//      .option("header","true")
//      .option("inferSchema","true")
//      .option("sep", ";")
//      .option("path","ruozedata-spark-sql/data/people.csv")
//      .load()
//      //      .load("ruozedata-spark-sql/data/people.csv")
//      .printSchema()
    //      .filter("age > 30")
    //      .write.format("csv")
    //      .option("header","true")
    //      .option("sep", "$")
    //        .mode(SaveMode.Overwrite)
    //        .save("out")

    //===============================================================

    // TODO...  text ==> Parquet
    // parquet
    //    val df = spark.read.format("text").load("ruozedata-spark-sql/data/people.txt")
    //        df.map(x => {
    //          val splits = x.getString(0).split(",")
    //          (splits(0),splits(1).trim.toInt)
    //        }).toDF("name","age")
    //            .write.format("parquet").mode(SaveMode.Overwrite).save("out")

    spark.read.load("out").show()
  }
 }
