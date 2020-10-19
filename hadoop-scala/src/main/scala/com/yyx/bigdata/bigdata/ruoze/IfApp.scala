package com.yyx.bigdata.bigdata.ruoze

/**
 * @author PK哥
 **/
object IfApp {

  def main(args: Array[String]): Unit = {

    val a = 11
    if(a % 2 ==0){
      println(s"$a 是偶数")
    }

    if(a % 2 ==0){
      println(s"$a 是偶数")
    } else {
      println(s"$a 是奇数")
    }

    val x = 10
    val y = 20
    val max = if(x > y) x else y
    println(max)


  }

}
