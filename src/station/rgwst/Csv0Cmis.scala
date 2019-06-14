package station.rgwst

import common.BaseType.StaEGMap
import common.TimeTransform.prevYmdhm_minute
import stat.Advect
import station.cmiss.CmBd

/**
  * 以csv0融合数据为背景，对cmis数据，用平流方程补缺
  * 1. 读入前10分钟csv0数据
  * 2. 读入当前cmis数据
  * 3. 用平流方程对当前数据补缺
  * Created by 何险峰，北京 on 17-1-2.
  */
object Csv0Cmis {
  def main(args: Array[String]): Unit = {
  }
  def apply(ymdhm: String): Advect = {
    Csv0Cmis(ymdhm,"csv0").adv
  }
}
case class Csv0Cmis(ymdhm: String,dataSrc:String) extends Csv with  CmBd{
  override def mkAdv : Advect = {
    val minu = 10
    val seconds = minu * 60
    val ymdhm_prev10m = prevYmdhm_minute(ymdhm,minu)
    // 使用前10分钟融合产品为背景场
    val prev      = mkArrEgMap(ymdhm_prev10m)
    val adv = if (prev == null )
      null
    else {
      //val egArrReal = mkArrEgMap4cmis(ymdhm)
      val egArrReal = null  // Csv0Cmis 仅在cimiss工作停止时使用
      Advect(ymdhm, prev, egArrReal, seconds)
    }
    adv
  }
  def mkArrEgMap4cmis(ymdhm_x: String) :  Array[StaEGMap] = {
    val elehArr = getEleHArr4DictA(ymdhm_x) //may == null
    if (elehArr == null) null else mkGradeMap(elehArr)
  }
}
