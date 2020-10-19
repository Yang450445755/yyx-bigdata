package com.yyx.bigdata.bigdata

/**
  * @author Aaron-yang
  * @date 2020/7/27 9:58
  */
object ScalaHomework {

  def main(args: Array[String]): Unit = {
    println("斐波那契数列 : " + fibonacciSequence(10))

    println("最初有多少个桃子 : " + monkeyQuestion(10,1))
  }


  /**
    * 斐波那契数列
    *   0、1、1、2、3、5、8、13、21、34
    * @param num
    */
  def fibonacciSequence (num:Int): Int ={

      if(num < 0){
        -1
      }else if(num == 0){
        0
      }else if(num == 1){
        1
      }else {
        fibonacciSequence(num - 1) + fibonacciSequence(num - 2)
      }
  }

  /**

    */
  /**
    * 一堆桃子，猴子第一天吃了其中一半，并再多吃了一个
    * 以后每天猴子都吃其中的一半+一个
    * 当吃到第十天(还没吃) 发现只有一个桃子
    * 问：最初有多少个桃子
    *  1  4  10
    * @param day  天数(未包含当天)
    * @param left  剩余数量
    * @return
    */
  def monkeyQuestion (day:Int,left:Int): Int ={
      if(day == 1){
        return left
      }
      monkeyQuestion(day-1,(left+1)*2)
  }

}
