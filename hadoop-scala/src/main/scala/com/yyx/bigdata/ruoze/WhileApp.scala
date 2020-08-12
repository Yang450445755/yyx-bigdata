package com.yyx.bigdata.ruoze

/**
 * @author PK哥
 **/
object WhileApp {
  def main(args: Array[String]): Unit = {


    var a = 1
    while(a <= 100) {
//      println(a)
      a += 1
    }


    var b = 1
    do {
      println(b)
      b += 1
    } while (b <= 100)

  }

}
