package station.cmiss

import common.TimeTransform._
import common.MyUtils._
import stat.Advect
import station._
import station.EleChk._
import station.rgwst.{Csv0Cmis, CsvReal, RgwstEleHLocal, RgwstEleHRemote}

/**
  * 目的：将不同来源Cimmis数据整合
  *
  * 1.从Cimmis提取小时，10分钟间隔数据
  * 2.用正点时次数据，对分钟数据补缺
  * 3.
  * Created by 何险峰，北京 on 16-7-9.
  */
object CmBind extends CmBd with EleHGradient{
  def main(args: Array[String]) {
    val ymdhm = "201610130620"
    val adv = this(ymdhm,"cmis")
    // val sta1 = 882047   // 5.2
    //val sta1 = 887009
    val sta1 = 889044  // 6.8
    val eleIdx = getEleHIdx("PRE_1h")
    val v = adv.mend(eleIdx)(sta1)
    val v1 = releasCtl(v.ev,"PRE_1h")
    println(s"${v.ev},${v.gv},$v1")
  }

  // cmis方式获取地面站数据
  def apply(ymdhm: String,dSrc : String): Advect = {
    val t0 = System.currentTimeMillis()
    val adv = dSrc match {
      case "cmis" =>
        val adv = mkAdvByCmis(ymdhm)  // cmis实况得到的结果最好
        if (adv == null || adv.real(getEleHIdx("TEM")).size < 1000){
          val msg = s"$ymdhm# 从$dSrc 获取数据失败呢。"
          logMsg(ymdhm, "2","1", "F", msg)
          sys.error(msg)
          //mkAdvByCsv0(ymdhm)          //使用前10分钟融合产品，平流外推得到本时次结果
        } else adv

      case "remote" =>
        RgwstEleHRemote(ymdhm)
      case "local" =>
        RgwstEleHLocal(ymdhm)
      case "csv0" | "csvreal" | "csvfit" =>
        CsvReal(ymdhm,dSrc)
      case _ =>
        val msg = "仅支持cmis,remote,local获取数据方式"
        logMsg(ymdhm, "2","1", "F", s"$ymdhm# $msg")
        sys.error(msg)
    }
    val recLen = adv.mend(getEleHIdx("PRE_1h")).size
    val dt = (System.currentTimeMillis() - t0)/1000
    val msg = s"采集地面数据，花费${dt}秒。可用雨量站 $recLen。"
    if (recLen > 20)
      logMsg(ymdhm, "2","1", "O", s"$ymdhm# $msg")
    else {
      val msg1 = s"$msg 本次处理将 may fail。"
      logMsg(ymdhm, "2","1", "F", s"$ymdhm# $msg")
      //sys.error(msg1)
    }
    adv
  }

  def mkAdvByCsv0(ymdhm: String) : Advect ={
    val adv = Csv0Cmis(ymdhm)
    adv
  }

  def mkAdvByCmis(ymdhm: String): Advect = {
    val minu = getMinutes(ymdhm)
    require(minu % 10 == 0)

    val elehArr = getEleHArr4DictA(ymdhm)
    val egMapArr0 = if (elehArr != null) mkGradeMap(elehArr) else {
      val msg = s"获取小时正点cimiss数据失败."
      logMsg(ymdhm, "2","1", "F", s"$ymdhm# $msg")
      sys.error(msg)
    }
    val msg0 = s"egMapArr0.length = ${egMapArr0.length} != $numEle4Ground"
    require(egMapArr0.length == numEle4Ground,msg0)
    val adv = minu match {
      case 0 =>
        val seconds = 60 * 60
        //统计表明，正点小时数据可能会延迟到达，需要用前一小时修补
        val elehArr_prevHour  = getEleHArr4DictA(prevYmdhm_hour(ymdhm,1))
        val egMapArr_prevHour = if (elehArr_prevHour != null) mkGradeMap(elehArr_prevHour) else null
        if (egMapArr_prevHour == null) null else Advect(ymdhm,egMapArr_prevHour, egMapArr0, seconds)
      case 10 | 20 | 30 | 40 | 50 =>
        val seconds = minu * 60
        val elehArr = getEleHArr4DictA(ymdhm)
        val egMapArrCur = if (elehArr == null) null else mkGradeMap(elehArr)
        if (egMapArrCur == null) null else Advect(ymdhm,egMapArr0, egMapArrCur, seconds)
    }
    adv
  }
}
