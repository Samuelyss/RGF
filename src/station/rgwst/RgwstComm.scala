package station.rgwst

import common.BaseType._
import common.MyConfig._
import common.MyUtils._
import common.TimeTransform._
import dict.StaDict
import stat.Advect
import station.EleChk._
import station.cmiss.EleH
import station.{EleHGradient, EleHOp}

/**
  * 为本地，远程访问提供公用方法
  * Created by 何险峰，北京 on 16-10-13.
  */
trait RgwstComm extends EleHOp with EleHGradient{
  //def getRgwstFnm(ymdhm:String):String
  def getRgwstFnm(ymdhm:String):String= {
    s"$rgwstDir/rgwst_${ymdhm}_005.txt"
  }

  def mkEgArr(ymdhm: String): Advect = {
    val ymdh0 = mkYmdh0(ymdhm)
    val elehArr = getElehArr(ymdhm)
    val egMapArrFull = mkGradeMap(elehArr)

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
      val elehArr_prevh = getElehArr(ymdh_prevh)
      val egMapArrFull_prevh = mkGradeMap(elehArr_prevh)
      val seconds = 1 * 60 * 60
      val adv = Advect(ymdhm, egMapArrFull_prevh, egMapArr_subset, seconds)
      adv
    } else {
      val minu = getMinutes(ymdhm)
      //用小时或上一小时数据外推
      val ymdh_prevh = if (minu == 0) prevYmdhm_hour(ymdh0,1) else ymdh0
      val elehArr_prevh = getElehArr(ymdh_prevh)
      val egMapArrFull_prevh = mkGradeMap(elehArr_prevh)
      val seconds = if (minu == 0) 1 * 60 * 60 else minu * 60
      val adv = Advect(ymdhm, egMapArrFull_prevh, egMapArrFull, seconds)
      adv
    }
  }
  def getElehArr(ymdhm: String): Array[EleH] = {
    val minut = getMinutes(ymdhm)
    val fnm = getRgwstFnm(ymdhm)
    val dictStaAll = StaDict.dictStaAll
    readTxtFile(ymdhm)
      .map(s => StrArr2EleH(s,minut))
      .filter(f => f != null)
      .filter(f => dictStaAll.keySet(f.sta))
  }

  def StrArr2EleH(s: Array[String],minut : Int): EleH = {
    val numCol = s.length
    def i(str: String): Int = str.trim.toFloat.toInt
    def f(str: String) = str.trim.toFloat
    def s2(ii: Int) = if (ii < 10) s"0$ii" else ii.toString
    val sta: Int = i(s(0))
    val lat: Float = f(s(1))
    val lon: Float = f(s(2))
    val alt: Float = ALTChk(s(3), lat, lon)
    val year: Int = i(s(4))
    val mon: String = s2(i(s(5)))
    val monInt = mon.toInt
    val day: String = s2(i(s(6)))
    val hour: String = s2(i(s(7)))
    val ymdh: String = s"$year$mon$day$hour"
    val pre1h0: Float = chk(s(10), 0.0f, PREMax) // PRE_1hChk(s(10))
    val pre1h1: Float = if (pre1h0.isNaN) pre1h0 else
      //在分钟报文中，pre1h记录小时以来，分钟累计雨量
      minut match {
      case 0 => pre1h0
      case 10 => pre1h0 * 6.0f
      case 20 => pre1h0 * 3.0f
      case 30 => pre1h0 * 2.0f
      case 40 => pre1h0 * 3.0f / 2.0f
      case 50 => pre1h0 * 6.0f / 5.0f
    }
    val pre1h2: Float = labelCtl(pre1h1,"PRE_1h")

    val pre10m: Float = if (pre1h0.isNaN) Float.NaN else  minut match {
      case 0  => pre1h0 / 6.0f
      case 10 => pre1h0
      case 20 => pre1h0 / 2.0f
      case 30 => pre1h0 / 3.0f
      case 40 => pre1h0 / 4.0f
      case 50 => pre1h0 / 5.0f
    }
    val pre10m1: Float = labelCtl(pre10m,"PRE_1h")

    val pre05m1: Float = if (pre1h0.isNaN) Float.NaN else labelCtl(pre10m / 2.0f,"PRE_1h")
    val pre05m2: Float = pre05m1

    val prs: Float = PRSChk(s(11),alt)

    val tem: Float = TEMChk(monInt,s(13))
    val prsSea: Float = PRS_SeaChk(s(12), prs, alt, tem)
    val rhu: Float = RHUChk(s(14))
    val ff: Float = WIN_S_Avg_2miChk(s(20))
    val ddd: Float = WIN_D_Avg_2miChk(s(19), ff)
    val uv: (Float, Float) = CalcuUV(ddd, ff)
    val u: Float = uv._1
    val v: Float = uv._2
    val ffmax: Float = WIN_S_Avg_2miChk(s(29))
    val dddmax: Float = WIN_D_Avg_2miChk(s(28), ffmax)
    val vism: Float = VISMChk(s(31))
    val vap: Float = VAPChk(s(42), tem, rhu)
    val dpt: Float = DPTChk(monInt,s(39), tem, vap)
    val qse: Float = calcuQse(prs, tem, dpt, vap)
    val gst: Float = GSTChk(monInt,s(41))
    val t5cm: Float = GSTChk(monInt,s(40))

    def may8x(eleIdx: Int): Float = {
      if (numCol > eleIdx) GSTChk(monInt,s(eleIdx)) else MissingFloat
    }
    val t10cm = may8x(80)
    val t15cm = may8x(81)
    val t20cm = may8x(82)
    val t40cm = may8x(83)
    val gtem = may8x(84)

    val numNaN = Array(prs, prsSea, tem, dpt, rhu, vap, pre1h2, ddd, ff,
      gst, t5cm, t10cm, t15cm, t20cm, t40cm, u, v, qse, gtem, vism, dddmax, ffmax)
      .count(f => f.isNaN)
    val eleh = if (numNaN == EleNms.length) null
    else EleH(
      ymdh, sta, lat, lon, alt, prs, prsSea, tem, dpt, rhu, vap, pre1h2, ddd, ff,
      gst, t5cm, t10cm, t15cm, t20cm, t40cm, u, v, qse, gtem, vism, dddmax, ffmax, pre10m1, pre05m1, pre05m2)
    eleh
  }

  def readTxtFile(ymdhm: String) : Array[Array[String]]={
    val numCol92: Int = 92
    val numCol80: Int = 80
    val numCol46: Int = 46
    //val minBytes = 1.1 * 1024 * 1024
    val sep = "\\s+"
    val fnm = getRgwstFnm(ymdhm)
    //logMsg(ymdhm,"WARN","Ok",s"读取$fnm")
    val lines =  fileLines(fnm)
      .map(_.trim)
      .zipWithIndex
    val numCol: Int = lines(0)._1.split(sep).length
    val msg = s"$fnm 到站行：${lines.length}, 每行记录数:$numCol"
    logMsg(ymdhm, "2","1", "I", s"$ymdhm# $msg")
    val lines0 : Array[String] =
      if (numCol == numCol92 || numCol == numCol80) lines.map(f => f._1) else  //每一行代表一个站点记录
      if (numCol == numCol46 ) {
        lines.partition(f => (f._2 % 2) == 0)
          .zipped
          .map((line1,line2) => s"${line1._1} ${line2._1}" )
      } else {
        println(s"$numCol not in $numCol92, $numCol80,$numCol46")
        lines.map(f => f._1)
      }
    val sArr = lines0(0).split(sep).map(_.trim)
    val isValidFile =
      sArr.length == numCol92 ||
        sArr.length == numCol80 ||
        sArr.length == numCol46
    val errMsg = s"${sArr.length} != $numCol92 or $numCol80 or $numCol46"
    if (! isValidFile) {
      val msg = s"$fnm $errMsg"
      logMsg(ymdhm, "2","1", "F", s"$ymdhm# $msg")
    }
    require(isValidFile,errMsg)
    lines0.map(_.split(sep).map(_.trim))
  }
}
