package common
import java.io.{File, FileWriter}
import java.net.InetAddress
import scala.math._
import grid.Dem
import scala.util.Random
import scala.collection.concurrent.TrieMap

/**
  * Created by 何险峰，北京 on 15-11-26 修改.
  */
object MyUtils {
  import BaseType._
  import MyConfig._
  import TimeTransform._
  import station.EleChk._
  import station.{EleChk, EleHOp, Wva}
  import dict.StaDict
  var hourStep = 1

  def s2f(s: String): Float = s.toFloat

  def randInt(mx: Int): Int =
    Random.nextInt(mx)


  def i2ii(i: Int): String = {
//    if (i < 10) s"0$i" else s"$i"
    "%02d".format(i)
  }

  def getTimeStr(fnm: String): String = {
    val pattern1 = "_[0-9]+_[0-9]+_".r
    val timeStrP = "[0-9]+_[0-9]+".r
    val nm = new File(fnm).getName
    val s1 = pattern1.findFirstIn(nm).get
    timeStrP.findFirstIn(s1).get
  }

  def fileLines(fnm: String): Array[String] = {
    val source = scala.io.Source.fromFile(fnm)
    try
      source.getLines.filter(f => f.length > 5).toArray
    finally
      source.close
  }

  def logMsg(ymdhm: String, stage: String, monitType: String, state: String, message: String) {
    val ymdh00 = mkYmdh0(ymdhm) + "00"
    if (logtype.indexOf(state) >= 0) {
      println(message)
      new File(s"$logAbsentDir").mkdirs()
      //1. 文件名rgf20170316000000.log
      val fnm = s"$logAbsentDir/rgf$ymdh00.log"
      val nowStr = getNowStr
      //对历史回算，不需要日志
      val needApend = dtInHour(nowStr.substring(0, nowStr.length - 2), ymdhm) < 10
      if (needApend) {
        val writer = new FileWriter(s"$fnm", needApend) //true表示以追加形式写文件
        //[S] [事件主机] [机器时间] [业务名称-环节代码] [事件类型] [事件状态] [事件信息]

        //业务名称-环节代码 :
        // rgf-1 卫星数据处理, rgf-2 cimiss, rgf-3 rgwst,
        // rgf-4 时间补缺， rgf-5 站点补缺，rgf-6 融合，rgf-7 统计,rgf-8 格点场,
        //monitType: 1 输入类, 2 处理类, 3 输出类
        //state : F(atal),M(ajor),W(arn),I(nfo),O(k),U(nknown)
        val ip = InetAddress.getLocalHost.getHostAddress
        val txt = s"""S $ip $nowStr rgf-$stage $monitType $state "$message"\r\n"""
        writer.write(txt)
        writer.flush()
        writer.close()
      }
    }
  }

  def mkTxtFilePath(ymdhm: String,rdir: String):String = {
    val ymd = getYmd(ymdhm)
    val year = getYear(ymdhm)
    new File(s"$rdir/$year/$ymd").mkdirs()
    val fnm = s"$rdir/$year/$ymd/$ymdhm.txt"
    fnm
  }

  def mkFile(ymdhm: String,rdir: String, msg: String,tp : String): Unit = {
    val fnm = mkTxtFilePath(ymdhm,rdir)
    logMsg(ymdhm, "8", "3", "O", s"$ymdhm# $tp$fnm ")
    val writer = new FileWriter(fnm, false) //true表示以追加形式写文件
    writer.write(msg)
    writer.flush()
    writer.close()
  }

  def logRMSE(ymdhm: String, rmse:String,pre:String) {
    //RMSE
    mkFile(ymdhm,logRMSEDir, rmse,"定量统计文件")
    //PRE_1h,PRE_10m,PRE_5m
    mkFile(ymdhm,logRMSEDir.replaceAll("RMSE", "PRE"), pre,"雨量定性统计文件")
  }


  def getCsvFnm(ymdhm: String, dataSrc: String): String = {
    val year = getYear(ymdhm)
    val ymd = getYmd(ymdhm)
    val dir = s"$csvDir/eleh/$dataSrc/$year/$ymd"
    new File(dir).mkdirs
    s"$dir/$ymdhm.csv"
  }

  def lstFile(root: String, ext: String): IndexedSeq[scala.reflect.io.Path] = {
    scala.reflect.io.Path(root).walkFilter(p => p.isDirectory || p.path.endsWith(ext)).toIndexedSeq
  }

  def wrt2txt(fnm: String, str: String): Unit = {
    val writer = new FileWriter(fnm, false) //true表示以追加形式写文件
    try {
      writer.write(str)
      writer.flush()
    } finally {
      writer.close
    }
  }

  import scala.collection.immutable.SortedMap
  def arr_idx(arr: AF1): SMFI = {
    val ai = arr.zipWithIndex.filter(f => f._1.toString != "NaN").toMap
    SortedMap.empty[Float, Int] ++ ai
  }

  def sqr(f: Float): Float = f * f

  //def eqMiss(v: Float): Boolean = v.equals(MissingFloat)
  def mkCoe(absC: Float, v: Float): Float = {
    if (v.isNaN) Float.NaN else {
      val sign = math.signum(v)
      val absV = abs(v)
      if (absV > absC) sign * absC else v
    }
  }

  def logDiff(x: Double, y: Double): Double = {
    //val (a, b) = if (x >= y) (x, y) else (y, x)
    val (a, b) = if (x < y) (x, y) else (y, x)
    -(b - a)
  }

  def gradient(eleNm: String, a: Float, b: Float, d: Float): Float = {
    if (a.isNaN || b.isNaN)
      Float.NaN
    else {
      val diff = if (eleNm.indexOf("PRE") >= 0 || eleNm.indexOf("VISM") >= 0)
        logDiff(a, b).toFloat
      else
        b - a
      diff / d
    }
  }

  def absDistSum(diff : Array[Float]):Float={
    diff
      .filter(f => !f.isNaN)
      .map(f => abs(f))
      .sum
  }

  def calcu_chk_gradient(eleNm: String, stawdArr: StawdArr, sta0: Int, sta_EleMap: StaOneEleMap):  Float = {
    val a = sta_EleMap(sta0)
    if (!a.isNaN) {
      val grades = stawdArr.filter(f => f.d >= Dem.step).map { f =>
        val b = sta_EleMap.getOrElse(f.sta, Float.NaN)
        val g: Float = gradient(eleNm, a, b, f.d)
        g
      }.filter(g => !g.isNaN)

      val len = grades.length
      val base = absDistSum(grades)
      val normGrades = grades.map(f => f / base)

      val sumGrade = normGrades.sum
      //val meanGrade = sumGrade / len

      val chkGrade = grades.sum / len
      val max = ele_gradient_ruler(a, eleNm)
      if (abs(chkGrade) > max)  Float.NaN else sumGrade
      //if (abs(meanGrade) > max) (Float.NaN, Float.NaN) else (meanGrade, sumGrade)
    } else Float.NaN
  }
  def ele_gradient_ruler(e: Float, elenm: String): Float = elenm match {
    case "TEM" => 20f //20
    case "RHU" => 30f

    case "VAP" => 20f

    case "DPT" => 30f

    case "GST" | "GTEM" => 30f //30
    case "T_5cm" | "T_10cm" |
         "T_15cm" | "T_20cm" | "T_40cm" => 15f //30

    case "WIN_D_Avg_2mi" | "DDDMAX" =>
      if (e < 20f || e > 340)
        360f + 25f
      else
        300f //720
    case "PRE_1h" | "PRE_10m" |
         "PRE_5mprev" | "PRE_5mpost" =>
       math.abs(logDiff(LN_Max_Pre, LN0)).toFloat

    case "VISM" => LN_Max_Vism
    case "PRS" => 300f //100
    case "PRS_Sea" => 100f
    case "QSE" => 40f //30
    case "U" | "V" |
         "WIN_S_Avg_2mi" | "FFMAX" => 40f //40
    case "hsd"   =>  100f
    case _ => 40f //40
  }

  def calcuCnt(stawdArr: StawdArr, sta_EleMap: StaOneEleMap, r: Float, isGe: Boolean): Int = {
    stawdArr.count { f =>
      val b: Float = sta_EleMap.getOrElse(f.sta, Float.NaN)
      if (isGe)
        b >= r
      else
        b <= r
    }
  }

  def calcuGradientAlt(eleNm: String, stawdArr: StawdArr, alt0: Float): Float = {
    if (!alt0.isNaN) {
      val grades = stawdArr.filter(f => abs(f.dz) > 10.0f).map { f =>
        (f.dz / (100f * 1000f)) / f.d
      }
      val base = absDistSum(grades)
      val normGrades = grades.map(f => f / base)

      val sumGrade = normGrades.sum
      sumGrade
      //val meanGrade = sumGrade / len
      //grades.sum / grades.length
    } else Float.NaN
  }

  def ddd2cos(ddd: Float): Float = {
    cos(ddd.toDouble.toRadians).toFloat
  }

  def egMap2xMap(egMap: StaEGMap, egStr: String): StaOneEleMap = {
    egMap.map { f =>
      val sta = f._1
      val egt = f._2
      val v = egStr match {
        case "ev" => egt.ev
        case "gv" => egt.gv
      }
      sta -> v
    }
  }


  //将观测值，梯度值用一维数组表示
  def arrStaEg2staAf1(sta_egMapArr: Array[StaEGMap], nEle: Int): StaElesMap = {
    require(sta_egMapArr.length == nEle, s"sta_egMapArr.length=${sta_egMapArr.length} != nEle=$nEle}")

    //形成一维EG
    def mkEGArr(sta: Int): Array[EG] = {
      val egs = Array.fill[EG](nEle)(null)
      for (eIdx <- (0 until nEle).par) {
        val sta_EG = sta_egMapArr(eIdx)
        val eg: EG = sta_EG.getOrElse(sta, EG(Float.NaN, Float.NaN))
        egs(eIdx) = eg
      }
      egs
    }

    def egArr2af1(eleh_eg: Array[EG]): AF1 = {
      val es = eleh_eg.map(_.ev) //实况数组
      val gs = eleh_eg.map(_.gv) //梯度数组
      es ++ gs
    }

    //println(s"sta_egMapArr.length = ${sta_egMapArr.length},nEle=${nEle}")
    val trie = TrieMap[Int,AF1]()
    StaDict.dictStaAll.par.foreach { s =>
      val sta = s._1
      val egtArr = mkEGArr(sta)
      val af1 = egArr2af1(egtArr)
      trie +=((sta, af1))
    }
    //println(s"sta_af1Map.size =${sta_af1Map.size} ")
    trie
  }

  def staAf12sta_elesMap(sta_af1Map: StaElesMap): StaElesMap = {
    val trie = TrieMap[Int,AF1]()
    sta_af1Map.par.foreach { f =>
      val sta = f._1
      val af1 = f._2.slice(0, numEle4Ground)
      trie +=((sta, af1))
    }
    trie
  }

  def staAf12txt(ymdhm: String, sta_af1Map: StaElesMap): String = {
    sta_af1Map.map { f =>
      val sta = f._1
      val info = StaDict.dictStaAll(sta)
      val txt = f._2
        .map(g => g.formatted("%1.2f"))
        .mkString(",")
      s"$ymdhm,$sta,${info.lat},${info.lon},${info.alt},$txt"
    }.mkString("\n")
  }

  def saveCsv(ymdhm: String, fnm: String, sta_elesMap: StaElesMap, dict: StaMap): Unit = {
    def countNaN(af1: AF1): Int = {
      af1.count(v => v.isNaN)
    }

    val sta_elesMap1 = sta_elesMap
      .filter(f => dict.keySet(f._1))
      .map(f => f._1 -> f._2.slice(0, numEle4Ground))
      .filter(f => countNaN(f._2) < numEle4Ground)
    //确保雨量，能见度< 0的，进行对数逆变换
    val sta_elesMap2 = EleChk.releasCtl_sta_eleMapArr(sta_elesMap1)
    common.MyUtils.wrt2txt(fnm, staAf12txt(ymdhm, sta_elesMap2))
  }

  /**
    * 定性统计：击中率，空报率，漏报率
    * $ymdh,${EleH.EleCNms(elehIdx)}
    */
  def preQualita(ymdhm: String, cnm: String, b: Float, pairs: Array[(Float, Float)]): String = {
    //Array[(实况，预报)])
    val str = if (pairs == null || pairs.length <= 0)
      ""
    else {
      val n01 = pairs.length
      val n1 = pairs.count(f => f._1 >= b)
      val f1 = pairs.count(f => f._2 >= b)
      val h1 = pairs.count(f => f._1 >= b && f._2 >= b)
      val m1 = pairs.count(f => f._1 >= b && f._2 < b)
      val fr1 = pairs.count(f => f._1 < b && f._2 >= b)
      val hit1 = if (f1 == 0) 0.0f else h1 / f1.toFloat
      val miss1 = if (n1 == 0) 0.0f else m1 / n1.toFloat
      val free1 = if (f1 == 0) 0.0f else fr1 / f1.toFloat
      //时间,降水名(降水级)，击中率，空报率，漏报率，总样本，样本1，预报1，击中1，空报1，漏报1
      f"$ymdhm,$cnm($b%.2fmm),$hit1%.2f,$free1%.2f,$miss1%.2f,$n01,$n1,$f1,$h1,$fr1,$m1"
    }
    str
  }

  /**
    * 皮尔逊线性相关系数
    */
  def pearsonCorrelation(pairs: Array[(Float, Float)]): Float = {
    val n = pairs.length
    val ff = pairs.unzip
    val f1 = ff._1
    val f2 = ff._2
    val sum1 = f1.sum
    val sum2 = f2.sum
    val sum1Sq = f1.foldLeft(0.0)(_ + sqr(_))
    val sum2Sq = f2.foldLeft(0.0)(_ + sqr(_))
    val pSum = pairs.foldLeft(0.0)((accum, element) => accum + element._1 * element._2)
    val numerator = pSum - (sum1 * sum2 / n)
    val denominator = sqrt((sum1Sq - sqr(sum1) / n) * (sum2Sq - sqr(sum2) / n))
    if (denominator == 0) MissingFloat else (numerator / denominator).toFloat
  }

  /**
    * 平均偏差
    */
  def BIAS(pairs: Array[(Float, Float)], isWD: Boolean = false): Float = {
    val n = pairs.length
    pairs.map{f =>
      val fact = f._1
      val fit  = f._2
      fit - fact}.sum / n
  }

  /**
    * 均方根误差
    */
  def MSE(pairs: Array[(Float, Float)], isWD: Boolean = false): Float = {
    val n = pairs.length
    sqrt(pairs.map(f => sqr(f._1 - f._2)).sum / n).toFloat
  }

  /**
    * 峰值信噪比
    */
  def PSNR(pairs: Array[(Float, Float)]): Float = {
    val mse = MSE(pairs)
    val psnr = 10 * log10(sqr(255) / mse)
    psnr.toFloat
  }

  /**
    * 雙線性插值. 見http://en.wikipedia.org/wiki/Bilinear_interpolation
    */
  def bilinear(lat: Double, lon: Double,
               x1: Double, x2: Double, y1: Double, y2: Double,
               q11: Double, q12: Double, q21: Double, q22: Double): Float = {
    val d = 1 / ((x2 - x1) * (y2 - y1))
    val dq11 = q11 * (x2 - lon) * (y2 - lat)
    val dq21 = q21 * (lon - x1) * (y2 - lat)
    val dq12 = q12 * (x2 - lon) * (lat - y1)
    val dq22 = q22 * (lon - x1) * (lat - y1)
    val v = (d * (dq11 + dq21 + dq12 + dq22)).toFloat
    v
  }

  /**
    * 指数滑动平均
    */
  def expMovingAverage(alpha: Float, xt: AF1): AF1 = {
    require(alpha > 0.0 && alpha <= 1.0, s"ExpMovingAverage found alpha = $alpha required > 0.0 and <= 1.0")
    if (xt.length <= 1) xt
    else {
      val alpha_1 = 1 - alpha
      var y = xt.head
      xt.map { x =>
        val z = x * alpha + y * alpha_1
        y = z
        z
      }
    }
  }

  def getDictFnm(fnm: String): String = {
    if (fnm.indexOf("csv") < 0) s"$fnm.csv" else fnm
  }

  def getNearStaWFnm(dictFnm: String): String = {
    s"${dictFnm}_knn_wd.csv"
  }

  def getNearStaDFnm(dictFnm: String): String = {
    s"${dictFnm}_knn_d.csv"
  }

  def getDLat(lat: Float): Float = {
    lat - 30f
  }

  def getDLon(lon: Float): Float = {
    lon - 100f
  }

  val R0: Double = 6378137.0
  val pi2: Double = Pi * 2

  def gaussW(R2: Double)(r2: Double): Float = {
    val t = (4 * r2) / R2     // 分布较 r2 / (4 *R2)  窄小，尖锐，方差小
    //val t = r2 / (4.0 * R2) // 分布较 (4 * r2) / R2 宽阔，平缓，方差大
    exp(-t).toFloat
  }
  //计算L 米为单位
  def calcuDist(sa_lat: Double, sa_lon: Double, sa_alt: Double)
               (sb_lat: Double, sb_lon: Double, sb_alt: Double): Double = {
    val dLon = toRadians(sb_lon - sa_lon)
    //距离单位为m
    val lat_a = toRadians(sa_lat)
    val lat_b = toRadians(sb_lat)
    val cosa = cos(lat_a)
    val sina = sin(lat_a)
    val cosb = cos(lat_b)
    val sinb = sin(lat_b)
    val cosb_a = cos(dLon)
    val da = abs(sb_alt - sa_alt)
    val dist = (R0 + da) * acos(cosa * cosb * cosb_a + sina * sinb)
    //val dist = (R0) * acos(cosa * cosb * cosb_a + sina * sinb)
    dist
  }

  def calcuDist(sa: LLPair)(sb: LLPair): Float = {
    val dist = calcuDist(sa._1, sa._2, 0)(sb._1, sb._2, 0)
    dist.toFloat
  }

  def calcuDist(sa: StaInfo)(sb: StaInfo): Float = {
    val dist = calcuDist(sa.lat, sa.lon, sa.alt)(sb.lat, sb.lon, sb.alt)
    dist.toFloat
  }

  def calcuDistDeg(sa: StaInfo)(sb: StaInfo): Float = {
    val dx = (sb.lon - sa.lon) * cos(toRadians(sa.lat)).toFloat
    val dy = sb.lat - sa.lat
    val sq = sqr(dx) + sqr(dy)
    math.sqrt(sq).toFloat
  }

  /*
    def LLA2xyz(lat : Double,lon:Double,alt:Double):Array[Double]={
      //验证：该公式有误
      //将球坐标转换为米制直角坐标
      //https://en.wikipedia.org/wiki/Spherical_coordinate_system
      val latr=toRadians(lat)
      val lonr=toRadians(lon)
      val r = R0 + alt
      val slat= sin(latr); val clat=cos(latr)
      val slon= sin(lonr); val clon=cos(lonr)
      val x = r * slat * clon
      val y = r * slat * slon
      val z = r * clat
      Array(x,y,z)
    }
  */
  def LLA2xyz(lat: Double, lon: Double, alt: Double): Array[Double] = {
    //将球坐标转换为米制直角坐标
    //https://wenku.baidu.com/view/7381b3d276a20029bd642d05
    val latr = toRadians(lat)
    val lonr = toRadians(lon)
    val r = R0 + alt
    val slat = sin(latr)
    val clat = cos(latr)
    val slon = sin(lonr)
    val clon = cos(lonr)
    val x = r * clat * clon
    val y = r * clat * slon
    val z = r * slat
    Array(x, y, z)
  }

  def xyz2LLA(x: Double, y: Double, z: Double): Array[Double] = {
    //将米制直角坐标转换为球坐标
    //https://en.wikipedia.org/wiki/Spherical_coordinate_system
    val r = sqrt(x * x + y * y + z * z) - R0
    val lat = acos(x / z)
    val lon = atan(y / z)
    Array(toDegrees(lat), toDegrees(lon), r)
  }

  def sqr(a: Double): Double = a * a


  def calcuDistXyz(a: Array[Double], b: Array[Double]): Double = {
    val sumd2 = a.zip(b).map(d => sqr(d._2 - d._1)).sum
    math.sqrt(sumd2)
  }

  //计算角度
  def calcuDelta(dL: Double, dLat: Double, lon_a: Float, lon_b: Float): Double = {
    val as = asin(R0 * dLat / dL)
    val delta = if (lon_b >= lon_a)
      (as - pi2) % pi2
    else
      (Pi - as) % pi2
    delta
  }

  //计算风向角度差
  def angleDiff(a: Double, b: Double): Float = {
    if (a.isNaN || b.isNaN) 0f else {
      val a0 = if (a < 90) a + 360 else a
      val b0 = if (b < 90) b + 360 else b
      val dif = ((b0 - a0 + 180.0) % 360.0).toFloat
      if (dif < 0) dif + 360f else dif
    }
  }

  //平均风向, 按风玫瑰图原理，不能够有权的作用
  def meanWD(wvas: Array[Wva]): Float = {
    if (wvas.nonEmpty) {
      val n = wvas.length
      val uvs = wvas
        .map { wv =>
          val w = wv.w
          val uv = d2uv(wv.eg.ev)
          val uvw = (w * uv._1, w * uv._2)
          uvw
        }
      val (us, vs) = uvs.unzip
      val meanU = us.sum / n
      val meanV = vs.sum / n
      val unit_WD = toDegrees(atan2(-meanU, -meanV))
      unit_WD.toFloat
    } else 0f
  }

  def d2uv(dd: Double): (Double, Double) = {
    if (dd.isNaN) (Double.NaN, Double.NaN)
    else {
      val rad = toRadians(dd)
      val u = -sin(rad) //u wind
      val v = -cos(rad) //v wind
      (u, v)
    }
  }
}
