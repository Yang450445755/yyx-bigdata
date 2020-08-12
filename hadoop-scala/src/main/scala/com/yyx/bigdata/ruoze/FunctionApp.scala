package com.yyx.bigdata.ruoze

/**
 * 【*****】
 **/
object FunctionApp {

  // 有返回值的定义
  def add(a:Int, b:Int) = {
    a + b // 方法体的最后一行 最为返回值
  }

  def add2 (a:Int, b:Int) =  a + b

  // 没有返回值
  def sayHello(): Unit ={
    println("sayHello")
  }

  def main(args: Array[String]): Unit = {

    // a +/-/*. b

    val a = 10
    val b = 20

//    val operator = "+"
//
//    if(operator == "+") {
//      println(a + b)
//    } else if(operator == "-"){
//      println(a - b)
//    }

    // 方法 抽象出来


    def max(x:Int, y:Int): Int = {  // 方法体
      if(x > y) {
        x
      } else {
        y
      }
    }

//    println(max(3,5))

//    println(add(2,5))


//    sayHello()
//    sayHello  //对于入参没有的，调用时()可以省略



//    fun1(10)

//    hello("ruoze")
//    hello()

//    loadConf()
//    loadConf("pk-defaults.conf")


//    println(speed(10, 5))
//    println(speed(time = 5, distance = 10))

//    println(sum(1, 2, 3))
//    println(sum(1, 2, 3,4,5,6))

//    println(sum(1 to 5: _*))

    val array = Array("Hadoop","Spark","Flink")
    printCourses(array: _*)

  }

  def printCourses(course:String*): Unit ={
    println(course)
  }

  // 1,2,3,4  2,3,4  5,6,7 ....

  def sum(nums:Int*) = {
    var result = 0
    for(num <- nums) {
      result += num
    }
    result
  }

  // 命名参数
  def speed(distance:Float, time:Float) = distance/time

  // 默认参数
  def hello(name:String = "pk"): Unit = {
    println(name)
  }

  def loadConf(conf:String = "spark-defaults.conf"): Unit = {
    println(conf)
  }

  def fun1(a:Int): Unit = {
    def fun2(b:Int): Unit = {
      println(s"${a+b}")
    }
    fun2(100)
  }
}
