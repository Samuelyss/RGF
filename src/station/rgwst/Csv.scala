package station.rgwst

import java.io.File

import common.BaseType.StaEGMap
import common.MyConfig.dictAFnm
import common.MyUtils._
import common.TimeTransform.{getMinutes, getMonth}
import dict.StaDict
import stat.Advect
import station.EleChk._
import station.EleHGradient
import station.cmiss.EleH

/**
  * 读取csv格式实况数据
  * Created by 何险峰，北京 on 17-1-2.
  */
trait Csv extends EleHGradient{
  val dataSrc : String
  val ymdhm   : String
  val adv     : Advect = mkAdv
  def mkAdv   : Advect

  def mkArrEgMap(ymdhm0: String) :  Array[StaEGMap] = {
    val elehArr = getElehArr(ymdhm0) //may == null
    if (elehArr == null) null else mkGradeMap(elehArr)
  }
  private def getElehArr(ymdhm0: String): Array[EleH] = {
    val minut = getMinutes(ymdhm0)
    val fnm = getCsvFnm(ymdhm0,dataSrc)
    if (new File(fnm).exists()){
      val dictStaAll = StaDict.dictStaAll
      readTxtFile(ymdhm0)
        .map(s => StrArr2EleH(s,minut))
        .filter(f => f != null)
        .filter(f => dictStaAll.keySet(f.sta))
    } else null
  }

  private def StrArr2EleH(s: Array[String],minut : Int): EleH = {
    def i(str: String): Int = str.trim.toFloat.toInt
    def f(str: String) = str.trim.toFloat

    val ymdhm0: String =      s(0)
    val sta: Int = i(s(1))
    val lat =      f(s(2))
    val lon =      f(s(3))
    val alt = ALTChk(s(4), lat, lon)
    val prs = PRSChk(s(5),alt)
    val prsSea = PRSChk(s(6),alt)
    val mon = getMonth(ymdhm)
    val tem: Float = TEMChk(mon,s(7))
    val dpt: Float = TEMChk(mon,s(8))
    val rhu: Float = RHUChk(s(9))
    val vap: Float = VAPChk(s(10),tem,rhu)
    val pre1h: Float = PRE_1hChk(lat,lon,s(11))
    val ff: Float = WIN_S_Avg_2miChk(s(13))
    val ddd: Float = WIN_D_Avg_2miChk(s(12), ff)
    val gst: Float = GSTChk(mon,s(14))
    val t5cm: Float = GSTChk(mon,s(15))
    val t10cm: Float = GSTChk(mon,s(16))
    val t15cm: Float = GSTChk(mon,s(17))
    val t20cm: Float = GSTChk(mon,s(18))
    val t40cm: Float = GSTChk(mon,s(19))
    val uv: (Float, Float) = CalcuUV(ddd, ff)
    val u: Float = uv._1                          // s(20)
    val v: Float = uv._2                          // s(21)
    val qse: Float = calcuQse(prs, tem, dpt, vap) // s(22)
    val gtem: Float = GSTChk(mon,s(23))
    val vism: Float = VISMChk(s(24))
    val ffmax: Float = WIN_S_Avg_2miChk(s(26))
    val dddmax: Float = WIN_D_Avg_2miChk(s(25), ffmax)
    val pre10m: Float = PRE_10mChk(s(27))
    val pre05m1: Float = PRE_5mChk(s(28))
    val pre05m2: Float = PRE_5mChk(s(29))
    val numNaN: Int = Array(prs, prsSea, tem, dpt, rhu, vap, pre1h, ddd, ff,
      gst, t5cm, t10cm, t15cm, t20cm, t40cm, u, v, qse, gtem, vism, dddmax, ffmax)
      .count(f => f.isNaN)
    val eleh: EleH = if (numNaN == EleNms.length) null
    else EleH(
      ymdhm0, sta, lat, lon, alt, prs, prsSea, tem, dpt, rhu, vap, pre1h, ddd, ff,
      gst, t5cm, t10cm, t15cm, t20cm, t40cm, u, v, qse, gtem, vism, dddmax, ffmax, pre10m, pre05m1, pre05m2)
    eleh
  }

  private def readTxtFile(ymdhm0: String) : Array[Array[String]]={
    val sep = ","
    val fnm = getCsvFnm(ymdhm0,dataSrc)
    val lines =  fileLines(fnm).map(_.trim)
    val numCol : Int = lines(0).split(sep).length

    val msg = s"$fnm 到站行：${lines.length}, 每行字段数:$numCol。"
    logMsg(ymdhm, "3","1", "O", s"$ymdhm0# $msg")
    lines.map(_.split(sep).map(_.trim))
  }
}
