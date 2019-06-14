package station

import common.BaseType._
import grid.Dem._
import common.MyConfig.{dictRFnm,is4chk}
import common.MyUtils.{saveCsv, _}
import grid.GdEleH
import station.EleChk._
import dict.StaDict

/**
  * 以客观分析后的格点场反算站点值+质量控制后的站点值：
  * 1. 输出反算站点值.doc/csv_fit
  * 2. 输出质控后站点值.doc/csv
  * 3. 输出质控统计量到logs/
  * Created by 何险峰，北京 on 16-4-6.
  */
object StatOutput extends EleHOp {
  val dictR: StaMap = StaDict.fetchDict(dictRFnm)
  def apply(csv0fnm: String, csvfitFnm: String, ymdhm: String, gdEleH: GdEleH): StaElesMap = {
    //val sta_elesFine1 = releasCtl_sta_eleMapArr(gdEleH.sta_elesFine)
    //反算站点值
    val sta_elesFit = getStaFit(gdEleH.as3)
    //    dataSrc match {
    //      case "cmis" | "remote" | "local" | "csvreal" | "csvfit" | "csv0" =>
    //质控补缺后站点值
    //使用dictStaAll,不仅可以满足质控需求，也满足后续本体推理需求
    saveCsv(ymdhm, csv0fnm, gdEleH.sta_elesFine, StaDict.dictStaAll)
    saveCsv(ymdhm, csvfitFnm, sta_elesFit, StaDict.dictStaAll)
    val msg = s"优化后文件$csv0fnm,站点拟合文件$csvfitFnm"
    logMsg(ymdhm, "7", "3", "O", s"$ymdhm# $msg")

    //质控统计量
    //logRMSE(ymdhm, mkRMSE_str(ymdhm, sta_elesFit, gdEleH.sta_elesFine))
    val (rmse, pre) = mkRMSE(ymdhm, gdEleH.sta_elesReal, sta_elesFit)
    logRMSE(ymdhm, rmse, pre)
    //    }
    sta_elesFit
  }

  /*
    输出气候极值检验、梯度检验后的实况站点
   */
  def mkCsvReal(ymdhm: String, sta_elesReal: StaElesMap,dSrc : String): StaElesMap = {
    if (sta_elesReal != null && dSrc != "csv0" && dSrc != "csvfit" && dSrc != "csvreal") {
      val fnm: String = getCsvFnm(ymdhm, "csvreal")
      val sta_elesMapReal1 = if (is4chk) {
        sta_elesReal.filter(f => f._1 > 60000)
      } else sta_elesReal
      saveCsv(ymdhm, fnm, sta_elesMapReal1, dictR)

      val msg = s"产生质量控制后文件$fnm,sta_elesMapReal.size=${sta_elesMapReal1.size}"
      logMsg(ymdhm, "7", "3", "O", s"$ymdhm# $msg")
      sta_elesMapReal1
    } else {
      val msg = s"sta_elesReal=null or dataSrc=$dSrc 包含csv, 质量控制后文件未存盘。"
      logMsg(ymdhm, "7", "3", "W", s"$ymdhm# $msg")
      sta_elesReal
    }
  }

  /*
    def getGridVal(eleIdx : Int,lat: Float, lon: Float, as2: AS2): Float = {
      val nm = EleNms(eleIdx)
      def s2f(i:Int,j:Int):Try[Float]={
        Try {
          val s = as2(i)(j)
          if (s == MISSING_SHORT) Float.NaN else nm match{
            case "PRE_1h" | "PRE_10m" =>  s / 100f
            case _ => s /10f
          }
        }
      }
      def hasNaN(fs : Array[Float]):Boolean={
        val n = fs.filter(f => ! eqMiss(f)).length
        n < 4
      }
      val lats = demLat.lats
      val lons = demLon.lons

      val nToLat = lats.to(lat).size
      val nToLon = lons.to(lon).size

      val (lat0,i)  = if (nToLat <= 0)
        (demLat.maxLat,0)
      else {
        val last = lats.to(lat).last
        (last._1, nLat - last._2)
      }

      val (lon0,j)  = if (nToLon <= 0)
        (demLon.maxLon,nLon - 1)
      else {
        val last = lons.to(lon).last
        (last._1,last._2)
      }

      val i1 : Int = if (i + 1 < nLat) i + 1 else i
      val j1 : Int = if (j - 1 < 0) j else j - 1

      val q11 = s2f(i1,j1).getOrElse(Float.NaN)
      val q12 = s2f(i,j1).getOrElse(Float.NaN)
      val q21 = s2f(i1,j1).getOrElse(Float.NaN)
      val q22 = s2f(i1,j).getOrElse(Float.NaN)

      val x1 = lon0 - Dem.step
      val x2 = lon0
      val y1 = lat0 - Dem.step
      val y2 = lat0

      val f = if (hasNaN(Array(q11,q12,q21,q22)))
        Float.NaN
      else{
        bilinear(lat,lon,x1,x2,y1,y2,q11,q12,q21,q22)
      }
      f
    }
  */
  val lats: SMFI = demLat.lats
  val lons: SMFI = demLon.lons

  def getStaFit(as3: AS3): StaElesMap = {
    def getGridVal(factor: Float, lat: Float, lon: Float, as2: AS2): Float = {
      val nToLat = lats.to(lat).size
      val nToLon = lons.to(lon).size
      val i = if (nToLat <= 0) 0 else nLat - lats.to(lat).last._2 - 1
      val j = if (nToLon <= 0) 0 else lons.to(lon).last._2
      if (as2(i)(j) == MISSING_SHORT) Float.NaN else as2(i)(j) * factor
    }

    val sta_elesMap = StaDict.dictStaAll.map { si =>
      val af1 = Array.fill[Float](numEle4Ground)(Float.NaN)
      val sta = si._1
      val lat = si._2.lat
      val lon = si._2.lon
      for (eIdx <- (0 until numEle4Ground).par) {
        val nm = EleNms(eIdx)
        val factor = if (nm.indexOf("PRE") >= 0)
          1f / 100f
        else
          1f / 10f
        af1(eIdx) = getGridVal(factor, lat, lon, as3(eIdx))
      }
      sta -> af1
    }
    sta_elesMap
  }

  /*
  雨量定性评分统计
   */
  def mkPreQuanStr(ymdhm: String, cnm: String, b: Array[Float], p0: Array[(Float, Float)]): String = {
    val p_str_arr = Array.fill[String](b.length)("")
    val p = p0.filter(f => !(f._1.isNaN || f._2.isNaN))
    for (i <- b.indices.par) {
      p_str_arr(i) = preQualita(ymdhm, cnm, b(i), p)
    }
    val str = p_str_arr.mkString("\n")
    println("时间,降水(降水级)，击中率，空报率，漏报率，总样本，样本1，预报1，击中1，空报1，漏报1")
    println(str)
    str
  }

  def mkRMSE(ymdhm: String, sta_elesReal0: StaElesMap, sta_elesFine0: StaElesMap): (String, String) = {
    val sta_elesFine = releasCtl_sta_eleMapArr(sta_elesFine0)
    val sta_elesReal = releasCtl_sta_eleMapArr(sta_elesReal0)
    val errs = Array.fill[String](numEle4Ground)("")
    val bh = Array(0.01f, 0.1f, 1.0f, 5.0f, 10.0f, 15.0f, 20.0f, 25.0f, 30.0f)
    val bm = Array(0.01f, 0.1f, 1.0f, 2.0f, 5.0f, 10.0f)
    //仅仅对国家站的数据进行统计 getDict(dict3Fnm)
    val dict = StaDict.dictStaAll

    val elesXsta: Array[Array[(Float, Float)]] = sta_elesReal
      .map { f =>
        val sta = f._1
        val fact1 = f._2
        val fit = sta_elesFine(sta)
        val pair = fact1 zip fit
        sta -> pair
      }
      .filter(f => dict.keySet(f._1))
      .values
      .toArray.transpose


    for (elehIdx <- (0 until numEle4Ground).par) {
      val elePairs = elesXsta(elehIdx).filter(f => !(f._1.isNaN || f._2.isNaN))
      val len = elePairs.length
      val enm = EleNms(elehIdx)
      val isWD = enm == "WIN_D_Avg_2mi" || enm == "DDDMAX"
      val bias = BIAS(elePairs, isWD).formatted("%.3f")
      val rmse = MSE(elePairs, isWD).formatted("%.3f")
      val corre = pearsonCorrelation(elePairs).formatted("%.3f")
      val str = s"$ymdhm,${EleCNms(elehIdx)},$bias,$rmse,$corre,$len"
      errs(elehIdx) = str
    }
    //时间,要素名,BIAS,RMSE,R,样本数\n ${errs.mkString("\n")})"""
    val rmse = errs.mkString("\n") + "\n"
    val p1h = getEleHIdx("PRE_1h")
    val pre1h_quanti_str = mkPreQuanStr(ymdhm, EleCNms(p1h), bh, elesXsta(p1h))
    val p10m = getEleHIdx("PRE_10m")
    val pre10m_quanti_str = mkPreQuanStr(ymdhm, EleCNms(p10m), bm, elesXsta(p10m))
    val p5m0 = getEleHIdx("PRE_5mprev")
    val p5m1 = getEleHIdx("PRE_5mpost")
    val pre5m_quanti_str = mkPreQuanStr(ymdhm, "R_5分钟雨量", bm, elesXsta(p5m0) ++ elesXsta(p5m1))
    val pre_str = s"$pre1h_quanti_str\n$pre10m_quanti_str\n$pre5m_quanti_str\n"
    (rmse, pre_str)
  }
}
