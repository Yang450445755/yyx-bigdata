/*
package ruoze

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author PK哥
 **/
object DataSourceApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)


//    val rdd = sc.parallelize(List(("BJ",30),("SH",31),("HZ",18)), 2)
//    rdd.foreach(x => {
//      classOf[com.mysql.jdbc.Driver]
//      val connection = DriverManager.getConnection("jdbc:mysql://ruozedata001:3306/ruozedata","root","!Ruozedata123")
//      println("..........................." + connection)
//
//      val pstmt = connection.prepareStatement("insert into stat(province, cnt) values(?,?)")
//      pstmt.setString(1, x._1)
//      pstmt.setInt(2, x._2)
//      pstmt.execute()
//
//      pstmt.close()
//      connection.close()
//    })


//    rdd.foreachPartition(partition => {
//      classOf[com.mysql.jdbc.Driver]
//      val connection = DriverManager.getConnection("jdbc:mysql://ruozedata001:3306/ruozedata","root","!Ruozedata123")
//      println("..........................." + connection)
//
//
//      connection.setAutoCommit(false)
//      var index = 0
//      val pstmt = connection.prepareStatement("insert into stat(province, cnt) values(?,?)")
//
//      partition.foreach(x => {
//        pstmt.setString(1, x._1)
//        pstmt.setInt(2, x._2)
//        pstmt.addBatch()
//
//        index += 1
//
//        // 1001
//        if(index % 1000 == 0) {
//          pstmt.executeBatch()
//        }
//
//      })
//
//      pstmt.executeBatch()
//      connection.commit()
//      connection.close()
//    })

//    new JdbcRDD(sc,() =>{
//      classOf[com.mysql.jdbc.Driver]
//      DriverManager.getConnection("jdbc:mysql://ruozedata001:3306/ruozedata","root","!Ruozedata123")
//    },
//    "select * from stat where cnt >= ? and cnt <=?",
//      20,31,1,
//      rs => (rs.getString(1), rs.getInt(2))
//    ).foreach(println)


    val configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM,"ruozedata001")
    configuration.set(TableOutputFormat.OUTPUT_TABLE, "user")

    val job = Job.getInstance(configuration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    // put
    sc.parallelize(List(("1","pk",31),("2","ruoze",32),("3","xingxing",19)))
        .map(x => {
          val put = new Put(Bytes.toBytes(x._1))
          put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("name"), Bytes.toBytes(x._2))
          put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("age"), Bytes.toBytes(x._3))
          (new ImmutableBytesWritable(), put)
        }).saveAsNewAPIHadoopDataset(job.getConfiguration)


    sc.stop()
  }

  case class Person(name:String, age:Int)

  def text(sc:SparkContext): Unit ={
    sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname","true")
    sc.hadoopConfiguration.set("fs.defaultFS","hdfs://ruozedata001:8020")

    val rdd = sc.parallelize(List(("pk",30),("ruoze",31),("xingxing",18)), 2)
    // TODO...
    val path = "hdfs://ruozedata001:8020/test"

    //    FileUtils.deleteOutput(sc.hadoopConfiguration, path)
    //    rdd.saveAsTextFile(path,classOf[BZip2Codec])
    //    rdd.saveAsNewAPIHadoopFile(path, classOf[NullWritable], classOf[Text],
    //      classOf[TextOutputFormat[NullWritable, Text]])
    //    sc.textFile(path).foreach(println)


    //    val rdd = sc.parallelize(List(("pk",30),("ruoze",31),("xingxing",18)), 2)
//    val path = "out"
    //    FileUtils.deleteOutput(sc.hadoopConfiguration, path)
    //    rdd.saveAsSequenceFile(path)
    //    sc.sequenceFile[String,Int](path).foreach(println)

    val p1 = Person("pk",30)
    val p2 = Person("ruoze",31)

    //    sc.parallelize(List(p1,p2)).saveAsObjectFile(path)

    sc.objectFile(path).foreach(println)
  }
}
*/
