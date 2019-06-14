package station

import common.BaseType._
import station.cmiss._
import scala.collection.concurrent.TrieMap
/**
  * Created by 何险峰，成都 on 16-2-2.
  */

trait EleHOp{
  val eps=1E-5f
  val EleCNms: List[String] = List[String](
    "P0_本站气压", "P1_海平面气压", "T_气温", "Td_露点", "Rh_相对湿度", "E_水汽压", "R_前1小时雨量", "DF0_2分钟风向", "DF1_2分钟风速",
    "T0_地面温度", "T05_05厘米地温", "T10_10厘米地温", "T15_15厘米地温", "T20_20厘米地温", "T40_40厘米地温",
    "U分量风", "V分量风", "Qse_假相当位温","TG_草地温度","V_能见度(km)","DF2_瞬间风向","DF3_瞬间风速",
    "R_前10分钟雨量","R_前5分钟雨量","R_后5分钟雨量"
  )
  val EleNms: List[String] = List[String](
    "PRS", "PRS_Sea", "TEM", "DPT", "RHU", "VAP", "PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi",
    "GST", "T_5cm", "T_10cm", "T_15cm", "T_20cm", "T_40cm", "U", "V", "QSE",
    "GTEM","VISM","DDDMAX","FFMAX","PRE_10m","PRE_5mprev","PRE_5mpost"
  )
  val numEle4Ground: Int = EleNms.length
  def rec2arr(e : EleH) : Array[Float]={
    Array(e.prs,e.prsSea,e.tem,e.dpt,e.rhu,e.vap,e.pre1h,
      e.winDAvg2mi,e.winSAvg2mi,e.gst,e.t5cm,e.t10cm,e.t15cm,
      e.t20cm,e.t40cm,e.u,e.v,e.qse,e.gtem,e.vism,e.dddmax,e.ffmax,e.pre10m,e.pre5mprev,e.pre5mpost)
  }

  def mkEleMapArr(elehArr: Array[EleH]): Array[StaOneEleMap] = {
    val eleMaps = Array.ofDim[TrieMap[Int, Float]](numEle4Ground)
    for (eleIdx <- (0 until numEle4Ground).par)
      eleMaps(eleIdx) = mkStaEleMap(elehArr, eleIdx)
    eleMaps
  }
  def getEleHIdx(eleNm : String):Int={
    EleNms.indexOf(eleNm)
  }
  /**
    * 获取站点 -> 非空单要素值
    *
    * @param elehArr : 站点实况Array[EleH]
    * @param eleIdx  : 要素枚举值
    * @return : 站点->非空单要素值
    */
  def mkStaEleMap(elehArr: Array[EleH], eleIdx: Int): StaOneEleMap = {
    val trie = TrieMap[Int, Float]()
    elehArr.filter(eleh => !getVal(eleh, eleIdx).isNaN)
      .par.foreach{eleh =>
        val sta = eleh.sta
        val v = getVal(eleh, eleIdx)
        trie +=((sta,v))
      }
    //println(s"${EleNms(eleIdx)},${eles.size}")
    trie
  }

  def getVal(eleh: EleH, eleIdx: Int): Float = {
    val v = EleNms(eleIdx) match {
      case "PRS" => eleh.prs
      case "PRS_Sea" => eleh.prsSea
      case "TEM" => eleh.tem
      case "DPT" => eleh.dpt
      case "RHU" => eleh.rhu
      case "VAP" => eleh.vap
      case "PRE_1h" => eleh.pre1h
      case "WIN_D_Avg_2mi" => eleh.winDAvg2mi
      case "WIN_S_Avg_2mi" => eleh.winSAvg2mi
      case "GST" => eleh.gst
      case "T_5cm" => eleh.t5cm
      case "T_10cm" => eleh.t10cm
      case "T_15cm" => eleh.t15cm
      case "T_20cm" => eleh.t20cm
      case "T_40cm" => eleh.t40cm
      case "U" => eleh.u
      case "V" => eleh.v
      case "QSE" => eleh.qse
      case "GTEM" => eleh.gtem
      case "VISM" => eleh.vism
      case "DDDMAX" => eleh.dddmax
      case "FFMAX"  => eleh.ffmax
      case "PRE_10m" => eleh.pre10m
      case "PRE_5mprev" => eleh.pre5mprev
      case "PRE_5mpost" => eleh.pre5mpost
      case _ => Float.NaN
    }
    v
  }
}
