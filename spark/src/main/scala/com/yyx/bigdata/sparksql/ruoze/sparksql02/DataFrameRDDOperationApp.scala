package com.yyx.bigdata.sparksql.ruoze.sparksql02

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author PK哥
 **/
object DataFrameRDDOperationApp {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()




//    inferReflection(spark)
    program(spark)

    spark.stop()
  }

  /**
   * 1) Create an RDD of Rows from the original RDD;
   * 2) Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
   * 3) Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
   */
  def program(spark:SparkSession): Unit = {
    val rdd: RDD[Row] = spark.sparkContext.textFile("ruozedata-spark-sql/data/people.txt")
      .map(_.split(",")).map(x => Row(x(0), x(1).trim.toLong))

    val schema =
      StructType(
          StructField("name", StringType, true) ::
           StructField("age", LongType, false) :: Nil)

    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show()
  }

  def inferReflection(spark:SparkSession): Unit = {
    import spark.implicits._
    val df = spark.sparkContext.textFile("ruozedata-spark-sql/data/people.txt")
      .map(_.split(",")).map(x => People(x(0), x(1).trim.toInt)).toDF

//    df.printSchema()
//    df.show()
    df.createOrReplaceTempView("people")
    spark.sql(
      """
        |select * from people
        |""".stripMargin).show()
  }

  case class People(name:String, age:Int)
}
