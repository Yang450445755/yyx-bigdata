package com.yyx.bigdata.a

/**
 * @author PK哥
 **/
object FlinkKafkaRedisApp {
  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromPropertiesFile(args(0))

    val stream = RuozedataFlinkKafkaUtils.createKafkaStream(parameters)
    val result = stream.flatMap(_.split(","))
      .map((_,1))
      .keyBy(x=>x._1)
      .sum(1)

    result.print()

    result.map(x => ("ruozedata-wc", x._1, x._2.toLong)).addSink(new RuozedataRedisSink)
    RuozedataFlinkKafkaUtils.env.execute(getClass.getCanonicalName)

  }
}
