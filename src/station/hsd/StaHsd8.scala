package station.hsd

import hsd.amtf.BandNc
import common.BaseType.{EG, StaEGMap, StaOneEleMap}
import common.MyConfig._
import common.MyUtils._
import common.TimeTransform._
import grid.hsd.{H9GdCase, HsdHxfGd}

import scala.collection.concurrent.TrieMap
import dict.{StaDict, StaNear}
/**
  * Created by hxf on 16-6-28.
  */
trait StaHsd8 {
  private case class StaHsdCase(ymdhm: String, sta: Int,
                        b01: Float, b02: Float, b03: Float, b04: Float, b05: Float, b06: Float,
                        b07: Float, b08: Float, b09: Float, b10: Float, b11: Float, b12: Float,
                        b13: Float, b14: Float, b15: Float, b16: Float)
  //val nBand: Int = HsdHxfGd.nBand
  //val EleCNms = List("段01", "段02", "段03", "段04", "段05", "段06", "段07", "段08", "段09", "段10", "段11", "段12", "段13", "段14", "段15", "段16")
  private val HsdEleNms = List("B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11", "B12", "B13", "B14", "B15", "B16")

  def mkHsdStaEGMapArr(ymdhm: String): Array[StaEGMap] ={
    val staHsds : Array[StaHsdCase] = if (hsd_ftpDir == "none")
      null
    else
      getStaHsdArr(ymdhm)
    val egMapArr : Array[StaEGMap]= if (staHsds == null) {
      val msg = s"获取卫星数据失败."
      logMsg(ymdhm, "1","1", "F", s"$ymdhm# $msg")
      null
    } else {
      val v = mkHsdGradeMap(staHsds)
      //val msg = s"获取卫星数据成功。StaHsd8.egMapArr.length=${v.length}"
      //logMsg(ymdhm, "1","1", "O", s"$ymdhm# $msg")
      v
    }
    egMapArr
  }


  /**
    * 将前10分钟的卫星数据作为正点卫星数据
    * 对dictStaAll站点，取卫星nBand个资料
    */
  private def getDat(ymdhm:String,ymdhm_10 : String): Array[StaHsdCase] = {
    val bxx : Array[H9GdCase] = HsdHxfGd(ymdhm_10)

    StaDict.dictStaAll.par.map {ss =>
      val staInfo = ss._2
      val sta = staInfo.sta
      val lat = staInfo.lat
      val lon = staInfo.lon
      val bv = Array.ofDim[Float](HsdHxfGd.nBand)
      for (b <- (0 until HsdHxfGd.nBand).par) {
        bv(b) = bxx(b).atLatLon(lat, lon)
      }
      StaHsdCase(ymdhm, sta,
        bv(0), bv(1), bv(2), bv(3), bv(4), bv(5), bv(6), bv(7),
        bv(8), bv(9), bv(10), bv(11), bv(12), bv(13), bv(14), bv(15))
    }.toArray
  }

  private def getStaHsdArr(ymdhm: String): Array[StaHsdCase] = {
    //////////////////////////////////////////////////////////////////////
    val ymdhm_10 = prevYmdhm_minute(ymdhm,10)
    //val msg = s"读取/下载卫星格点场..."
    //logMsg(ymdhm, "1","1", "I", s"$ymdhm_10# $msg")
    val (_,exists) = HsdHxfGd.chkYmdhmFileExists(ymdhm_10)
    if (exists) {
      val v = getDat(ymdhm,ymdhm_10)
      v
    } else {
      //val t0 = System.currentTimeMillis()
      BandNc(hsd_ftpDir,ymdhm_10,hsdDir,"/tmp/ramdisk")
      val v = getDat(ymdhm,ymdhm_10)
      //val dt = (System.currentTimeMillis() - t0)/1000
      val (fnm1, exists1) = HsdHxfGd.chkYmdhmFileExists(ymdhm_10)
      if (exists1) {
        //val msg = s"$fnm1 length = ${v.length},花费${dt}秒。"
        //logMsg(ymdhm, "1","1", "O", s"$ymdhm_10# $msg")
      } else {
        val msg = s"$fnm1 读取/下载卫星格点场失败。"
        logMsg(ymdhm, "1","1", "W", s"$ymdhm_10# $msg")
      }
      v
    }
  }

  /**
    * 取邻近站点间的空间梯度值
    */
  private def mkHsdGradeMap(stahsdArr: Array[StaHsdCase]): Array[StaEGMap] = {
    // 对每个通道要素建立站点->值字典
    def mkStaEleMap( hsdEleIdx: Int): StaOneEleMap = {
      val trie = TrieMap[Int, Float]()
      val eles = stahsdArr
        .filter(stah => !getVal(stah, hsdEleIdx).isNaN)
        .map(stah => trie += ((stah.sta, getVal(stah, hsdEleIdx))))
      trie
    }
    //对所有通道要素建立站点->值字典
    def mkHsdEleMapArr: Array[StaOneEleMap] = {
      val eleMaps = Array.fill[TrieMap[Int, Float]](HsdHxfGd.nBand)(null)
      for (hsdeleIdx <- 0 until HsdHxfGd.nBand) {
        eleMaps(hsdeleIdx) = mkStaEleMap(hsdeleIdx)
      }
      eleMaps
    }

    val hsdEleMapArr = mkHsdEleMapArr
    val egMapArr = Array.ofDim[StaEGMap](HsdHxfGd.nBand)
    val dictNear = StaNear.fetchStaNear(dictAFnm)
    for (hsdEleIdx <- 0 until HsdHxfGd.nBand) {
      val staEleMap = hsdEleMapArr(hsdEleIdx)
      val trie = TrieMap[Int,EG]()
      staEleMap.par.foreach { f =>
        val sta = f._1
        val e = f._2
        val grade: Float = calcu_chk_gradient("hsd",dictNear(sta), sta, staEleMap)
        val eg = if (e.isNaN) EG(Float.NaN, Float.NaN) else EG(e, grade)
        trie +=((sta, eg))
      }
      egMapArr(hsdEleIdx) = trie
    }
    egMapArr
  }

  /*
  def rec2arr(h: StaHsdCase): Array[Float] = {
    Array(h.b01, h.b02, h.b03, h.b04, h.b05, h.b06, h.b07, h.b08, h.b09, h.b10, h.b11, h.b12, h.b13, h.b14, h.b15, h.b16)
  }
  */
  private def getVal(staHsdCase: StaHsdCase, hsdIdx: Int): Float = {
    HsdEleNms(hsdIdx) match {
      case "B01" => staHsdCase.b01
      case "B02" => staHsdCase.b02
      case "B03" => staHsdCase.b03
      case "B04" => staHsdCase.b04
      case "B05" => staHsdCase.b05
      case "B06" => staHsdCase.b06
      case "B07" => staHsdCase.b07
      case "B08" => staHsdCase.b08
      case "B09" => staHsdCase.b09
      case "B10" => staHsdCase.b10
      case "B11" => staHsdCase.b11
      case "B12" => staHsdCase.b12
      case "B13" => staHsdCase.b13
      case "B14" => staHsdCase.b14
      case "B15" => staHsdCase.b15
      case "B16" => staHsdCase.b16
      case _ =>
        //println(s" getVal fail ${EleNms(hsdIdx)} in StaHsd8.getVal object")
        Float.NaN
    }
  }
}
