package station.rgwst

import common.BaseType.StaEGMap
import common.MyConfig.{is4chk}
import common.MyUtils.logMsg
import common.TimeTransform.{getMinutes, mkYmdh0, prevYmdhm_hour}
import stat.Advect
import station.EleHOp

/**
  * 读入质量控制后的地面实况要素
  * Created by 何险峰，北京 on 16-11-26.
  */
object CsvReal {
  def main(args: Array[String]): Unit = {
    //this("201609070400","csvreal")
  }
  def apply(ymdhm: String,dataSrc:String): Advect = {
    CsvRealInst(ymdhm,dataSrc).adv
  }
}
case class CsvRealInst(ymdhm: String,dataSrc:String) extends Csv with EleHOp{
  override def mkAdv: Advect = {
    val ymdh0 = mkYmdh0(ymdhm)
    val egMapArrFull: Array[StaEGMap] = mkArrEgMap(ymdhm) // may null
    if (is4chk) {
      val msg = s"独立检验，去除国家站..."
      logMsg(ymdhm, "2","1", "I", s"$ymdhm# $msg")

      //去除国家站后的子集
      val egMapArr_subset = Array.ofDim[StaEGMap](numEle4Ground)
      for (i <- (0 until numEle4Ground).par) {
        val v = egMapArrFull(i).filter(g => g._1 >= 60000)
        egMapArr_subset(i) = v
      }
      //用上一小时数据外推
      val ymdh_prevh = prevYmdhm_hour(ymdh0, 1)
      val egMapArrFull_prevh = mkArrEgMap(ymdh_prevh)

      val seconds = 1 * 60 * 60
      val adv = Advect(ymdhm, egMapArrFull_prevh, egMapArr_subset, seconds)
      adv
    } else {
      val minu = getMinutes(ymdhm)
      //用小时或上一小时数据外推
      val ymdh_prevh = if (minu == 0) prevYmdhm_hour(ymdh0, 1) else ymdh0
      val egMapArrFull_prevh = mkArrEgMap(ymdh_prevh)
      val seconds = if (minu == 0) 1 * 60 * 60 else minu * 60
      val adv = Advect(ymdhm, egMapArrFull_prevh, egMapArrFull, seconds)
      adv
    }
  }
}
