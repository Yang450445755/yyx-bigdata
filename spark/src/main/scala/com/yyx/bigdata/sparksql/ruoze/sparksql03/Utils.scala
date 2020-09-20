package com.yyx.bigdata.sparksql.ruoze.sparksql03

import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType}

object Utils {

  def castTo(value:String, dataType:DataType) = {
    dataType match {
      case _: DoubleType => value.toDouble
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }

}
