package com.yyx.bigdata.bigdata.sparksql

import org.apache.spark.{SparkConf, SparkContext}

import java.util.{Calendar, Date}

/**
  * @author Aaron-yang
  * @date 2020/9/20 11:00
  */
object ContinuousLogin {



  /**
    * pk,2020-09-20
    * pk,2020-09-20
    * pk,2020-09-21
    * pk,2020-09-22
    * pk,2020-09-24
    * pk,2020-09-25
    * ruoze,2020-09-20
    * ruoze,2020-09-22
    * ruoze,2020-09-24
    * ruoze,2020-09-25
    * ruoze,2020-09-26
    * ruoze,2020-09-27
    * ruoze,2020-09-29
    * ruoze,2020-09-30
    *
    * 统计得到  连续登陆的天数
    *
    *
    * 思路:
    * 1. map分组   ( 人名,日期 )  去除value相同的值
    * 2. 按时间排序
    * 3. 日期相差一天就放入同一个集合 , 不是就新增一个集合来放置
    * 3. 最后得到 map(人名 , 连续日期集合)的集合
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName("yyxnb")
    val sc = new SparkContext(sparkConf)

    val map = sc.textFile("C:\\new_java\\yyx-bigdata\\spark\\src\\main\\scala\\com\\yyx\\bigdata\\sparksql\\login")
      .map(line => {
        val split = line.split(",")
        (split(0), split(1))
      })
      .groupByKey()
      .map(s=>{
        //时间排序,去重
        val sortedList = s._2.toSet.toArray.sorted

        //获得连续的天数
        val continuousDateList = getContinuousDate(sortedList)

        //返回
        (s._1,continuousDateList)
      }).foreach(println)

    sc.stop()
  }

  /**
    * 2020-09-20
    * 2020-09-21
    * 2020-09-22
    * 2020-09-24
    * 2020-09-25
    *
    *
    * 2020-09-20
    * 2020-09-22
    * 2020-09-24
    * 2020-09-25
    * 2020-09-26
    * 2020-09-27
    * 2020-09-29
    * 2020-09-30
    *
    * @param sortedList
    * @return
    */

  def getContinuousDate(sortedList: Array[String]): Array[(String, String)] = {
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd")

    var calendar = Calendar.getInstance()

  /*  val result = new Array[String](sortedList.length)
    for (i <- 0 to sortedList.length){

        val continuousDate = new StringBuilder
        //取下一个比上一个大一天就放入结果集合
        if(dateFormat.parse(sortedList(i+1)).compareTo(dateFormat.parse(sortedList(i)))==1){
          continuousDate.append(sortedList(i))
        }

        result+continuousDate.toString()
        println(continuousDate)
      }

    println(result)
    result*/
    var index = 0
    val tuples = sortedList.map(x => { //分组内每组依次减去index，获取起始连续登陆时间
      calendar.setTime(format.parse(x))
      calendar.add(Calendar.DAY_OF_YEAR, -index)
      index += 1
      (x, format.format(calendar.getTime))
    })

    tuples.foreach(println)
    tuples
  }

}
