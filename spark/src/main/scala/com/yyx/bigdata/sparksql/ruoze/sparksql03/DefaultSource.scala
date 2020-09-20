package com.yyx.bigdata.sparksql.ruoze.sparksql03

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class DefaultSource extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val path = parameters.get("path")

    path match {
      case Some(p) => new RuozedataTextDataSourceRelation(sqlContext,p)
      case _  => throw new IllegalArgumentException("path is required...")
    }
  }
}
