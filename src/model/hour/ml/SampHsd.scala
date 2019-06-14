package model.hour.ml

import common.BaseType.StaElesMap
import common.MyUtils.logMsg
import common.TimeTransform.prevYmdhm_minute
import station.hsd.StaHsdExt

/**
  * 建立
  * Created by 何险峰，北京 on 16-12-5.
  */
case class SampHsd(ymdhm : String,dSrc:String) extends TSamp{
  def getSta_af1:  Array[StaElesMap]={
    // 如果本时次卫星资料缺，就用上时次的
    val sta_af14hsd0 = getSta_af14hsd(ymdhm)
    val sta_af14hsd1 = if (sta_af14hsd0 == null) {
      val sta_af14hsd_prev =  getSta_af14hsd(prevYmdhm_minute(ymdhm, 10))
      sta_af14hsd_prev
    } else sta_af14hsd0

    val sta_af14hsd2 = if (sta_af14hsd1 == null) {
      val msg = s"两次卫星数据处理失败，本次使用地面观测数据为优化依据."
      logMsg(ymdhm, "6","1", "M", s"$ymdhm# $msg")
      null
      //SampBP(ymdhm).getSta_af1
    } else sta_af14hsd1
    //println(s"sta_af14hsd2.size=${sta_af14hsd2.size}")
    Array(sta_af14hsd2)
  }
  private def getSta_af14hsd(ymdhm0: String): StaElesMap = try {
    StaHsdExt(ymdhm0)
  } catch {
    case e: Exception =>
      val msg = s"卫星数据接收失败." + e
      logMsg(ymdhm0, "6","1", "M", s"$ymdhm0# $msg")
      // 不要弹出，留给下一步处理
      // throw new Exception(msg)
      null
  }
}
