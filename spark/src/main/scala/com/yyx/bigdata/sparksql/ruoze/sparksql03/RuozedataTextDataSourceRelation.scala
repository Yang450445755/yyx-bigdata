package com.yyx.bigdata.sparksql.ruoze.sparksql03

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types._

class RuozedataTextDataSourceRelation(
                                       @transient val sqlContext: SQLContext,
                                       path: String)
  extends BaseRelation
    with TableScan
  with InsertableRelation
    with Logging {

  override def schema: StructType = StructType(
    StructField("id", LongType, false) ::
      StructField("name", StringType, false) ::
      StructField("gender", StringType, false) ::
      StructField("salary", DoubleType, false) ::
      StructField("comm", DoubleType, false) :: Nil
  )

  override def buildScan(): RDD[Row] = {
    logError("这是若泽数据自定义数据源实现:buildScan")
    // TODO...  rdd + schemaField

    val info = sqlContext.sparkContext.textFile(path)  // RDD[String]
    val schemaFields: Array[StructField] = schema.fields

    info.map(_.split(",").map(_.trim)).map(x => x.zipWithIndex.map {
      case (value, index) => {
        val colName = schemaFields(index).name
        // String ==> StructFiled(name, datatype, nullable)
        Utils.castTo(if (colName.equalsIgnoreCase("gender")) {
          if (value == "1") {
            "男"
          } else if (value == "2") {
            "女"
          } else {
            "未知"
          }
        } else {
          value
        }, schemaFields(index).dataType)
      }
    }).map(x=>Row.fromSeq(x))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .save(path)
  }
}
