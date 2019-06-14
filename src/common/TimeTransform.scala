package common

import amtf.Ver
import common.MyConfig.delayMinutes
import common.MyUtils.logMsg
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by hxf on 15-12-11.
  */
object TimeTransform {
  def main(args: Array[String]) {
    val ymdh1="201609070400"
    val s = prevYmdhm_minute(ymdh1,10)
    println(s)
  }
  /**
    * 分钟差,以小时为单位
    */
  def dtInHour(timeStr1 : String,timeStr2 : String):Float={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val t1 = DateTime.parse(timeStr1, ymdhm_fmt).getMillis
    val t2 = DateTime.parse(timeStr2, ymdhm_fmt).getMillis
    val dt0 = if (t1 > t2) t1 - t2 else t2 - t1
    val dts = dt0 / 1000.0
    val dtm = dts / 60.0
    val dth = dtm / 60.0
    dth.toFloat
  }
  def dtInMinu(timeStr1 : String,timeStr2 : String):Int={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val t1 = DateTime.parse(timeStr1, ymdhm_fmt).getMillis
    val t2 = DateTime.parse(timeStr2, ymdhm_fmt).getMillis
    val dt0 = if (t1 > t2) t1 - t2 else t2 - t1
    val dts = dt0 / 1000.0
    val dtm = dts / 60.0
    math.round(dtm).toInt
  }

  def getNowStr : String={
    val cur = DateTime.now
    val ymdhm = cur.toString("yyyyMMddHHmmss")
    ymdhm
  }

  def getYmdhmDirStrPair(timeStr : String):(String,String)={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val mdhm_fmt = DateTimeFormat.forPattern("yyyy/MM/dd/HHmm")
    val fnm_fmt = DateTimeFormat.forPattern("MMddHHmm.md")
    (mdhm_fmt.print(datetime),fnm_fmt.print(datetime))
  }
  def getMillis(timeStr : String):Long ={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    DateTime.parse(timeStr, ymdhm_fmt).getMillis
  }

  def prevYmdhm_minute(timeStr : String, nMinute : Int):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val dtPrev = datetime.minusMinutes(nMinute)
    val ymdhmPrevStr = ymdhm_fmt.print(dtPrev)
    ymdhmPrevStr
  }

  def nextYmdhm_minute(timeStr : String, nMinute : Int=1):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val dtNext = datetime.plusMinutes(nMinute)
    val ymdhmNextStr = ymdhm_fmt.print(dtNext)
    ymdhmNextStr
  }

  def nextYmdhm_hour(timeStr : String, nHour : Int=1):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val dtNext = datetime.plusHours(nHour)
    val ymdhmNextStr = ymdhm_fmt.print(dtNext)
    ymdhmNextStr
  }

  def prevYmdhm_hour(timeStr : String, nHour : Int):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val dtPrev = datetime.minusHours(nHour)
    val ymdhmPrevStr = ymdhm_fmt.print(dtPrev)
    ymdhmPrevStr
  }

  def getYmdh0(timeStr : String):String={
    val ymdh0_fmt = DateTimeFormat.forPattern("yyyyMMddHH00")
    val datetime = DateTime.parse(timeStr, ymdh0_fmt)
    val ymdh0Str = ymdh0_fmt.print(datetime)
    ymdh0Str
  }

  def mkYmdh0(timeStr : String):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val ymdh0_fmt = DateTimeFormat.forPattern("yyyyMMddHH00")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val ymdh0Str = ymdh0_fmt.print(datetime)
    ymdh0Str
  }

  def mk_ymdhm(timeStr: String): String = {
    val ymd_hms_fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.0")
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymd_hms_fmt)//
    val ymdhmStr = ymdhm_fmt.print(datetime)
    ymdhmStr
  }

  def mk_ymdhmSlash(timeStr: String): String = {
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val ymdhSlash_fmt = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")
    val datetime = DateTime.parse(timeStr,ymdhSlash_fmt)
    val ymdhmStr = ymdhm_fmt.print(datetime)
    ymdhmStr
  }

  def ymdhm2y_m_d_hm(ymdhmStr: String):String={
    val y_m_d_hms_fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:00")
    val datetime = DateTime.parse(ymdhmStr, DateTimeFormat.forPattern("yyyyMMddHHmm"))
    val y_m_d_hmStr = y_m_d_hms_fmt.print(datetime)
    //println(s"===========${y_m_d_hStr}=============")
    y_m_d_hmStr
  }

  def mk_ymdhm(time0: DateTime) = {
    time0.toString("yyyyMMddHHmm")
  }

  def mk_ymdh_sta(time0: DateTime, sta: String) = {
    (mk_ymdhm(time0), sta.trim)
  }

  def getMinutes(ymdhmStr: String):Int={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    datetime.getMinuteOfHour
  }
  def getHours(ymdhmStr: String):Int={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    datetime.getHourOfDay
  }
  def getMonth(ymdhmStr: String):Int={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    datetime.getMonthOfYear
  }
  def getYear(ymdhmStr: String):Int={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    datetime.getYear
  }
  def getYmd(ymdhmStr: String):String={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    datetime.toString("yyyyMMdd")
  }
  def getYm(ymdhmStr: String):String={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    datetime.toString("yyyyMM")
  }

  def getCosDayMinu(ymdhmStr: String):(Float,Float)={
    val fmt1 = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymdhmStr, fmt1)
    val d = (datetime.dayOfYear.get * 360.0 / 365.0).toRadians
    val m = (datetime.minuteOfDay.get * 360.0 / 1440.0).toRadians
    val dcos = - math.cos(d).toFloat
    val mcos = - math.cos(m).toFloat
    (dcos,mcos)
  }

  def calcuDelay(cycleMinute : Int): (String, Int, Int) = {
    val cur = DateTime.now.minusHours(8) // 北京时转世界时
      .minusMinutes(delayMinutes) // 可用资料时
    val ymdh = cur.toString("yyyyMMddHH")
    val curMinu = cur.minuteOfHour().get
    val minute0: Int = (curMinu / 10) * 10 // 10分钟为单位
    val mmStr = if (minute0 == 0) "00" else minute0.toString
    val ymdhm = s"$ymdh$mmStr"

    val nextMinu = cycleMinute - curMinu % cycleMinute
    val msg = s"${Ver.ver}.将处理$ymdh, $minute0 分数据. ${nextMinu}分钟后，启动下时次任务"
    logMsg(ymdhm, "1","1", "I", s"$ymdhm# $msg")
    (ymdhm, nextMinu, minute0)
  }

  def hjn(): Unit ={
    val t=new DateTime()
    t.toString("yyyy-MM-dd HH:mm:ss")

  }
}
