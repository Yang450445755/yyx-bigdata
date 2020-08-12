package com.yyx.bigdata.ruoze

import scala.io.StdIn
import scala.util.control.Breaks.{break, breakable}
/**
 * @author PK哥
 **/
object ForApp {

  def main(args: Array[String]): Unit = {

    val a = "abc"
    for(ele <- a) {
      // println(ele + 0)
    }

//    for(i <- 1 to 3) { // to []
//      println(i)
//    }

    // 1 to 3  ==> 1.to(3) ==> 1 to 3
    for(i <- 1.to(10,2)) {
//      println(i)
    }

    for(i <- 10 until 1 by -1) { // until [)
//       println(i)
    }

    for(i <- 1.to(10).reverse) {
//            println(i)
    }

    // 10以内的奇数  循环守卫
    for(i <- 1 to 10 if i % 2 == 0) {

//      println(i)
    }

    Range(1,10,2)

    // break

//    val i = StdIn.readInt()
//    println("----" + i)

    var flag = true
    val x = 7  // 质数

//    for(i <- 2 until x) {
//      if(x%i == 0) {
//        flag = false
//      }
//    }
//
//    println(flag)

    // 我工作中没有用过，面试题中出现过在scala中如何退出循环
//    breakable{
//          for(i <- 2 until x) {
//            if(x%i == 0) {
//              flag = false
//              break
//            }
//          }
//    }
//
//    println(flag)



    for(i<-1 to 9; j<-1 to i) {
//        print(s"$j * $i = ${i*j}\t")

//      if(i == j) println()
    }


    // *****  * 2
    for(i<-1 to 9)  yield  i * 2


    val s = for (ele <- "ruozedata") yield ele.toString.toUpperCase + ele
    s.mkString("")
  }
}
