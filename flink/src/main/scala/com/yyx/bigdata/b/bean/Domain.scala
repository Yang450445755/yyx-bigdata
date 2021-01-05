package com.yyx.bigdata.bean

/**
 * @author PK哥
 **/
object Domain {

  case class AccessLog(domain:String, userId:String, time:String)

  case class AccessLogV2(time:String, domain:String, province:String, city:String)

}
