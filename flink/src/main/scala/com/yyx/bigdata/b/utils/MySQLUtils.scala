package com.yyx.bigdata.utils

import java.sql.{Connection, DriverManager}

/**
 * @author PK哥
 **/
object MySQLUtils {

  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://ruozedata001:3306/ruozedata","root","!Ruozedata123")
  }

  def closeConnection(connection:Connection): Unit = {
    if(null != connection) {
      connection.close()
    }
  }

}
