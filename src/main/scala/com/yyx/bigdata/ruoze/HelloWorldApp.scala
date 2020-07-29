package com.yyx.bigdata.ruoze

/**
 * 这是类上的注释
 */
object HelloWorldApp {

  /**
   * main就是入口点
   * 这是main方法的注释
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 调用Scala的SDK： 单行注释
//    println("Hello World....")

    /** 调用Java的SDK： 多行注释 */
//    System.out.println("Hello Java")

    // 在Scala中每一行语句并不需要显示的加上;

//    println("Hello World...."); println("Hello World...."); println("Hello World....")


    val money = 25678
    println(money)

    printf("格式化输出: %d\n", money)
    printf("格式化输出: %f\n", math.Pi)
    printf("格式化输出: %.3f\n", math.Pi)

    val name = "PK"
    val age = 30
    println(name + "==>" + age)

    // 字符串插值
    // User  name  age    user.name  user.age
    println(s"$name==>$age-2")
    println(s"$name==>${age-2}")


    // 多行输出

    val welcome = "欢迎来到若泽数据" +
      "这里是大数据实战班"
    println(welcome)

    val welcome2 =
      """
        |欢迎来到若泽数据
        |这里是大数据实战班
        |""".stripMargin
    println(welcome2)



    val sql =
      """
        |
        |select
        |deptno, count(1) cnt
        |from
        |emp
        |group by deptno
        |
        |
        |""".stripMargin
    println(sql)
  }

}
