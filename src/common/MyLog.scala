package common

import org.joda.time.DateTime

/**
  * 设计依据：监控平台日志格式标准规范
  * Created by 何险峰，维也纳  on 2017/5/2.
  */
object MyLog {
  val prjNm = "RGF"
  val zone = 8 //need minus hours
  val fnmTemp = s"$getNowStr.log"
  val cycle = 3


  def getNowStr() : String={
    val cur = DateTime.now.minusHours(zone)
    val ymdhm = cur.toString("yyyyMMddHHmmss")
    ymdhm
  }

}
