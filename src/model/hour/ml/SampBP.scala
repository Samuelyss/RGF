package model.hour.ml

import common.BaseType.StaElesMap
import common.MyConfig._
import common.MyUtils.logMsg
import station.EleHBP

/**
  * 通过信任传播建立使用数据集
  * Created by 何险峰，北京 on 16-12-5.
  */
case class SampBP(ymdhm : String,  dSrc: String) extends TSamp{
  def getSta_af1 : Array[StaElesMap]={
    val eleHBP = try{
      EleHBP(ymdhm,dSrc)
    } catch {
      case e: Exception =>
        val msg = s"$dSrc 方式, 获取站点数据失败." + e
        logMsg(ymdhm, "6","1", "F", s"$ymdhm# $msg")
        sys.error(msg)
    }
    Array(eleHBP.sta_elesBP,eleHBP.sta_elesReal)
  }
}
