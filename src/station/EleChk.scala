package station

import common.BaseType._
import common.MyUtils._
import grid.Dem

import scala.math.log
import common.MyConfig._
/**
  * Created by 何险峰，北京 on 15-12-11.
  */
case class Wva(w: Float, eg: EG, dz: Float, wv: Float, eg0: EG, d: Float)

object EleChk extends EleHOp{
  val Y0                 = 25.0f
  val PREMax             = 150.0f
  val VISMMax            = 20.0f //30.0f
  val LN0: Float         = -8f //log(0.01/Y0).toFloat   //  = -7.82
  val LN_Max_Pre : Float = log(PREMax/Y0).toFloat  //  = -1.82
  val LN_Max_Vism: Float = log(VISMMax/Y0).toFloat  //  = 30--> 0.18 , 50km-->0.693, 100 --> 1.38629


  def ln(rain: Double): Float = {
    log(rain / Y0).toFloat
  }
  val LN001: Float = ln(0.01)
  val LN002: Float = ln(0.02)
  val LN003: Float = ln(0.03)
  val LN004: Float = ln(0.04)
  val LN005: Float = ln(0.05)
  val LN007: Float = ln(0.07)
  val LN01: Float = ln(0.1)
  val LN02: Float = ln(0.2)
  val LN03: Float = ln(0.3)
  val LN1  : Float = ln(1.0)
  val LN10: Float = ln(10.0)
  val LN20: Float = ln(20.0)
  val LN40: Float = ln(40.0)
  val LN60: Float = ln(60.0)
  val LN70: Float = ln(60.0)
  val LN17: Float = ln(17.0)
  val minPRS = 520f
  val maxPRS = 1085f

  def chk0(s: String): Float = {
    val f: Float =
      if (s == null || s.isEmpty ||
        s.contains("999999") || s.contains("999998") ||
        s == "NaN") MissingFloat
      else if (s == "999990.0") 0.09f
      else if (s.contains("998.0") && s.length >= 8) {
        s.substring(3).toFloat
      } else if (s.contains("997.0") && s.length >= 8) {
        s.substring(3).toFloat * (-1.0f)
      } else s.toFloat
    if (f > 900000.0f) MissingFloat else f
  }


  def chk(s: String, less: Float, great: Float): Float = {
    val vv = chk0(s)
    val v = if (vv.isNaN || vv < less || vv > great) MissingFloat else vv
    v
  }

  def ALTChk(altStr: String, lat: Float, lon: Float): Float = {
    val altDem = Dem.getAlt(lat, lon)
    val alt0 = if (altStr == null) altDem else {
      val alt1 = chk(altStr, 10f, 4500f)
      if (alt1.isNaN) altDem else alt1
    }
    alt0
  }

  //全国自动气象站实时观测资料三级质量控制系统研制.pdf
  def PRSChk(p: String, alt: Float): Float = {
    if (alt < 0)
      chk(p, 955f, maxPRS)
    else if (alt >= 0 && alt < 50)
      chk(p, 945f, 1075f)
    else if (alt >= 50 && alt < 100)
      chk(p, 935f, 1075f)
    else if (alt >= 100 && alt < 150)
      chk(p, 935f, 1065f)
    else if (alt > 900)
      chk(p, 535f, 1000f)
    else
      chk(p, 535f, 1050f)
  }

  def main(args: Array[String]) {
    val p0 = calcuE(34.1f, 53f)
    println(p0)
  }

  def PRS_SeaChk(prsSea: String, prs: Float, alt: Float, tem: Float): Float = {
    val p0 = chk(prsSea, minPRS, maxPRS)
    if (p0.isNaN && !prs.isNaN && !tem.isNaN) {
      val cp0 = calcuP0(prs, alt, tem)
      if (cp0 > maxPRS) prs else cp0
    } else p0
  }

  // 全国自动气象站实时观测资料三级质量控制系统研制.pdf
  def TEMChk(mon: Int, t: String): Float = {
    val (min, max) = mon match {
      case 1 | 2 | 10 | 11 | 12 => (-55f, 40f)
      case 3 | 4 => (-50f, 45f)
      case 5 | 6 | 7 | 8 | 9 => (-19f, 65f)
    }
    chk(t, min, max)
  }

  def DPTChk(mon: Int, dpt: String, tem: Float, e: Float): Float = {
    def calcuTd: Float = {
      val ps = math.log(e / 6.1115)
      (272.55 * ps / (22.452 - ps)).toFloat
    }

    //来自QX/T 118-2010
    val td0 = TEMChk(mon, dpt)
    val td = if (td0 > tem || td0 >= 45) Float.NaN else td0
    if (td.isNaN && !tem.isNaN && !e.isNaN && tem < 25.0f) calcuTd else td
  }

  def RHUChk(rhu: String): Float = {
    val r0 = chk(rhu, -2f, 110.0f)
    if (r0 < 0f)
      0f
    else if (r0 > 100f)
      100f
    else
      r0
  }

  def VAPChk(e: String, tem: Float, rhu: Float): Float = {
    val c0 = chk(e, 4.0f, 70.0f)
    //val c0 = chk(e, 20.0f, 80.0f)
    if (c0.isNaN && !tem.isNaN && !rhu.isNaN) calcuE(tem, rhu) else c0
  }

  def GSTChk(mon: Int, t: String): Float = {
    TEMChk(mon, t)
  }

  def PRE_Chk(s: String, max: Float): Float = {
    if (s.contains("99.9"))
      MissingFloat
    else {
      val r = chk(s, 0.0f, max)
      val r_ln = labelCtl(r, "PRE_1h")
      r_ln
    }
  }

  def PRE_1hChk(lat: Float, lon: Float, pre_1h: String): Float = {
    val max =
      if (lon <= 100f || lat > 41f)
        80f
      else if (lat < 25f)
        PREMax
      else 145f
    PRE_Chk(pre_1h, max)
  }

  def PRE_10mChk(pre_10m: String): Float = {
    PRE_Chk(pre_10m, 30f)
  }

  def PRE_5mChk(pre_5m: String): Float = {
    PRE_Chk(pre_5m, 20f)
  }

  def WIN_D_Avg_2miChk(d: String, ff: Float): Float = {
    val d0 = chk(d, 0.0f, 360.0f)
    if (d0.isNaN && !ff.isNaN) 0.0f else d0
  }

  //来自QX/T 118-2010
  def WIN_S_Avg_2miChk(v: String): Float = {
    chk(v, 0.0f, 75.0f)
  }

  def VISMChk(v: String): Float = {
    val vmax = 500 * 1000f
    val v0 = chk(v, 0.0f, vmax)
    val v1 = if (v0.isNaN) v0 else{
      val v2 = v0 / 1000.0f
      val v3 = if (v2 > VISMMax) VISMMax else v2
      labelCtl(v3, "VISM")
    }
    v1
  }

  import scala.math._

  def calcuP0(ps: Float, h: Float, tc: Float): Float = {
    // 来自气压观测规范：QX/T 49-2007
    val T0 = 273.1592
    val a = 18400f
    val b = a * (1 + tc / T0)
    val e = h / b
    val d = pow(10, e)
    val p0 = ps * d
    p0.toFloat
  }

  def calcuEs(t: Float): Float = {
    //董双林 崔宏光.饱和水汽压计算公式的分析比较及经验公式的改进[J].应用气象学报,1992,3(4):501~508
    //第(19)公式
    val Aw = 19.802
    val Ai = 23.622
    val Bw = 17.885
    val Bi = -5.5087
    //val Cw = 2.311 * 1E-4
    val Ci = 1.098 * 1E-4
    val T = t + 273.16
    val x = if (t > 0) (Aw * t) / (T - Bw) else (Ai * t) / (T - Bi + Ci * T * T)
    val e = 6.11139 * exp(x)
    e.toFloat
  }

  def calcuE(t: Float, rhu: Float): Float = {
    val Es = calcuEs(t)
    rhu / 100.0f * Es
  }

  /**
    * U,V风分解
    */
  def CalcuUV(dd: Float, ff: Float): (Float, Float) = {
    if (dd.isNaN || ff.isNaN) (MissingFloat, MissingFloat)
    else {
      val rad = toRadians(dd)
      val u = (-ff * sin(rad)).toFloat //u wind
      val v = (-ff * cos(rad)).toFloat //v wind
      (u, v)
    }
  }

  /**
    * 假相当位温
    */
  def calcuQse(p: Float, t: Float, td: Float, e: Float): Float = {
    val T0 = 273.15926f
    // 丁一汇,《天气动力学中的诊断分析方法》 P47
    val Qse0 = if (p.isNaN || t.isNaN || td.isNaN || t.isNaN) MissingFloat
    else {
      val q = 0.622 * e / (p - 0.378 * e)
      val TT = t + T0
      val t1 = 0.28586 * log(1000.0 / p)
      val t2 = 2500.0 * q / (338.52 - 0.24 * TT + 1.24 * td)
      (TT * exp(t1 + t2) - T0).toFloat
    }
    Qse0
  }

  def elehTrim(eleIdx: Int, eval: Float, isGrid: Boolean = false): Float = EleNms(eleIdx) match {
    case "PRS" | "PRS_Sea" =>
      if (eval < minPRS) minPRS else if (eval > maxPRS) maxPRS else eval
    case "WIN_S_Avg_2mi" | "FFMAX" => if (eval < 0f) 0f else if (eval > 75f) 75f else eval
    case "WIN_D_Avg_2mi" |  "U" | "V"|"DDDMAX"  => eval
    case "PRE_1h" | "PRE_10m" | "PRE_5mprev" | "PRE_5mpost"  => //假定雨量,能见度使用对数表示
      if (isGrid) {
        if (eval < 0f) 0f else if (eval > PREMax) PREMax else eval
      } else {
        if (eval < LN0) LN0 else if (eval > LN_Max_Pre) LN_Max_Pre else eval
      }
    case "VISM" =>
      if (isGrid) {
        if (eval < 0f) 0f else if (eval > VISMMax) VISMMax else eval
      } else {
        if (eval < LN0) LN0 else if (eval > LN_Max_Vism) LN_Max_Vism else eval
      }

    case "QSE" =>
      if (eval < -50f) -50f else if (eval > 120f) 120f else eval
    case "TEM" | "DPT" | "GST" | "T_5cm" | "T_10cm" | "T_15cm" | "T_20cm" | "T_40cm" | "GTEM" =>
      if (eval < -50f) -50f else if (eval > 70f) 70f else eval
    case "RHU" =>
      if (eval < 0f) 0f else if (eval > 100f) 100f else eval
    case "VAP" =>
      if (eval < 5f) 5f else if (eval > 70f) 70f else eval
  }

  def calcuPressure(p: Float, dAlt: Float, w: Float): Float = {
    val coeP = 0.88f
    p * w * math.pow(coeP, dAlt / 1000f).toFloat
  }


  /**
    * 学习算法前，将标签值范围控制在合理区间
    *
    */
  def labelCtl(v: Float, eleNm: String): Float = {
    if (v.isNaN) v else eleNm match {
      case "PRE_1h" | "PRE_10m" | "PRE_5mprev" | "PRE_5mpost" | "VISM" =>
        //val v1 = v.formatted("%.1f").toFloat //输入雨量有可能有两位小数，仅保留1位
        v match {
          case 0 => LN0
          case _ =>
            val v2 = log(v / Y0).toFloat
            if (v2 < LN0)
              LN0
            else if (eleNm.contains("PRE") && v2 > LN_Max_Pre)
              LN_Max_Pre
            else if (eleNm.contains("VISM") && v2 > LN_Max_Vism)
              LN_Max_Vism
            else  v2
        }
      case _ => v
    }
  }

  def labelCtl(v: Float, eleIdx: Int): Float = {
    labelCtl(v, EleNms(eleIdx))
  }

  /**
    * releasCtl 的逆运算。模型套用时，保留原始极值信息
    */
  def releasCtl(v: Float, eleNm: String): Float = {
    if (v.isNaN) v else
      eleNm match {
        case "PRE_1h" | "PRE_10m" | "PRE_5mprev" | "PRE_5mpost" | "VISM" =>
          val expFit = Y0 * exp(v).toFloat
          if (expFit < 0.02f)
            0f
          else if (eleNm.contains("PRE") && expFit > PREMax)
            PREMax
          else if (eleNm.contains("VISM") && expFit > VISMMax)
            VISMMax
          else expFit
        case _ => v
      }
  }

  def releasCtlByIdx(v: Float, eleIdx: Int): Float = {
    releasCtl(v, EleNms(eleIdx))
  }

  private def existMinus(sta_elesMap: StaElesMap): Boolean = {
    val pre_1h_idx = getEleHIdx("PRE_1h")
    val arr = sta_elesMap
      .map { f =>
        val af1 = f._2
        af1(pre_1h_idx)
      }
      .filter(f => !f.isNaN)
    val mn = if (arr.nonEmpty)
      arr.min
    else
      1.0f // 数据为空，故不存在负号
    mn < -1.0f
  }

  def releasCtl_sta_eleMapArr(sta_elesMap: StaElesMap): StaElesMap = {
    if (!existMinus(sta_elesMap))
      sta_elesMap
    else {
      val pre_1h_idx     = getEleHIdx("PRE_1h")
      val vism_idx       = getEleHIdx("VISM")
      val pre_10m_idx    = getEleHIdx("PRE_10m")
      val pre_5mprev_idx = getEleHIdx("PRE_5mprev")
      val pre_5mpost_idx = getEleHIdx("PRE_5mpost")
      val sta_elesMap1   = sta_elesMap.map { f =>
        val sta = f._1
        val af1 = f._2.clone()
        //val af1 = f._2
        af1(pre_1h_idx) = releasCtlByIdx(af1(pre_1h_idx), pre_1h_idx)
        af1(vism_idx) = releasCtlByIdx(af1(vism_idx), vism_idx)
        af1(pre_10m_idx) = releasCtlByIdx(af1(pre_10m_idx), pre_10m_idx)
        af1(pre_5mprev_idx) = releasCtlByIdx(af1(pre_5mprev_idx), pre_5mprev_idx)
        af1(pre_5mpost_idx) = releasCtlByIdx(af1(pre_5mpost_idx), pre_5mpost_idx)
        sta -> af1
      }
      sta_elesMap1
    }
  }

  def mean2(p1: Float, p2: Float): Float = {
    if (p1.isNaN && p2.isNaN)
      p1
    else if (p1.isNaN)
      p2
    else if (p2.isNaN)
      p1
    else (p1 + p2) / 2f
  }

  def zRate(eleNm: String, a: Float, b: Float, dz: Float, w: Float,isGrid : Boolean): Float = {
    if (a.isNaN && b.isNaN)
      Float.NaN
    else if (a.isNaN || isGrid)
      zRate_const(eleNm, b, dz, w)
    else {
      val g = gradient(eleNm, a, b, dz)
      val dg0 = dz * g
      val s = dg0.compare(0)
      val dg = if (math.abs(dg0) > maxEleDz)
        s * maxEleDz
      else
        dg0
      val bdw0 = (b + dg)*w
      val bdw = if (eleNm.contains("PRE") && bdw0 < LN0) LN0 else bdw0
      bdw.toFloat
    }
  }

  def zRate_const(eleNm: String, b: Float, dz: Float, w: Float): Float = {
    /**
      * 当eg0==null时，仅能够依赖邻近站点资料补缺，递减率只好用常量替代
      */
    def ca: Float = {
      val c0 = eleNm match {
        case "RHU" =>
          0.3f
        case "TEM" =>
          0.30f
        case "DPT" | "GST" | "GTEM" | "VAP" =>
          0.4f
        case "QSE" | "T_5cm" | "T_10cm" | "T_15cm" | "T_20cm" | "T_40cm" =>
          0.5f
        case "U" | "V" | "WIN_S_Avg_2mi" | "FFMAX" =>
          if (abs(b) < 1f) 0f else -0.1f
        case _ => 0f
      }
      val c1 = c0 / 100f
      (b + dz * c1) * w
    }
    ca
  }

  def eg2e(eg: EG): Float = {
    if (eg != null) eg.ev else Float.NaN
  }

  def eg2g(eg: EG): Float = {
    if (eg != null) eg.gv else Float.NaN
  }

  /**
    * 计算邻近站海拔高度的影响
    *
    */
  //val setPositive = Set("WIN_S_Avg_2mi" ,"FFMAX","VAP" , "RHU")
  def actAlt(eleIdx: Int, wvas: Array[Wva],isGrid : Boolean): EG = {
    //val set1 = Set("VISM" , "PRE_1h" , "PRE_10m" , "PRE_5mprev" , "PRE_5mpost" )
    val sumw = wvas.map(_.w).sum
    val sumwv0 = wvas.map(_.wv).sum
    val eleNm = EleNms(eleIdx)
    val sumwv_dz: Float = eleNm match {
      case "PRS" | "PRS_Sea" =>
        wvas.map { f =>
          val v1 = f.eg.ev
          calcuPressure(v1, -f.dz, f.w)
        }.sum
      case "TEM" | "GST" | "GTEM" | "T_5cm" | "T_10cm" | "T_15cm" | "T_20cm" | "T_40cm" |
           "DPT" | "VAP" | "QSE" | "RHU" |
           "U" | "V"
           | "WIN_S_Avg_2mi" | "FFMAX"
           | "VISM" | "PRE_1h" | "PRE_10m" | "PRE_5mprev" | "PRE_5mpost" =>
        wvas
          .map { f =>
            //邻近点值
            val b = eg2e(f.eg)
            //本站值
            val a = eg2e(f.eg0)
            val w = f.w
            zRate(eleNm, a, b, f.dz, w,isGrid)
          }
          .filter(f => !f.isNaN)
          .sum

      //case "WIN_D_Avg_2mi" | "DDDMAX" =>   meanWD(wvas)

      case _ => sumwv0 //"WIN_D_Avg_2mi" | "DDDMAX" |  "VISM" | "PRE_1h" | "PRE_10m" | "PRE_5mprev" | "PRE_5mpost" =>
    }

    val e_z = sumwv_dz / sumw
    val e_0 = sumwv0 / sumw
    val e_p = if (e_0 == 0f) 0f else abs((e_z - e_0) / e_0)
    val e = if (e_p > 0.618f) e_0 else e_z

    //val e = sumwv_dz / sumw
    val g = wvas.filter(f => !f.eg.gv.isNaN).map(f => f.w * f.eg.gv).sum / sumw
    EG(elehTrim(eleIdx, e), g)
  }
}
