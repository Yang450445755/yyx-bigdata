package com.yyx.bigdata.bigdata.ruoze

/**
 * Byte: -128 127
 **/
object DataTypeApp {

  def main(args: Array[String]): Unit = {

    //val a1:Byte = 128

    val a = 10
    val b = 1000000000000000L

    //精度丢失
    val c = 3.141592688458585F  //float
    val d = 3.141592688458585


    // Char
    val c1: Char = 'a'
    c1.toInt

    val c2: Char = ('a' + 1).toChar

    val f = true



    // 自动转换
    val x = 1 + 2.0

    val y = 10L
//    val y1:Int = y


    val z = 3.15.toInt

    (10 * 4.5 + 8 * 1.5).toInt

    // 是什么数据类型  转成什么数据类型  *****
    10.isInstanceOf[Int]
    10.asInstanceOf[Double]

  }
}
