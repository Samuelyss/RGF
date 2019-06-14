package station

import common.BaseType._
import EleChk._
import common.MyUtils._
import dict.{StaDict, StaNear}

import scala.collection.concurrent.TrieMap

/**
  * 以Array[EleH]为基础，进行梯度检验并更新，得到Array[StaEGMap]
  * Created by 何险峰，成都 on 16-2-2.
  */
trait EleHGradient extends EleHOp {
  import station.cmiss.EleH
  def sta_af1_2_egMapArr(ymdhm: String, sta_af1: StaElesMap): Array[StaEGMap] = {
    val elehArr = sta_af1.map { f =>
      val sta = f._1
      val es = f._2
      val i = StaDict.dictStaAll(sta)
      EleH(ymdhm, sta, i.lat, i.lon, i.alt,
        es(0), es(1), es(2), es(3), es(4), es(5), es(6), es(7), es(8), es(9),
        es(10), es(11), es(12), es(13), es(14), es(15), es(16), es(17), es(18), es(19),
        es(20), es(21), es(22), es(23), es(24))
    }.toArray
    mkGradeMap(elehArr)
  }

  val ground_tem_NmSet = Set("GTEM","GST", "T_5cm", "T_10cm", "T_15cm", "T_20cm", "T_40cm")
  def mkGradeMap(elehArr: Array[EleH]): Array[StaEGMap] = {
    val eleMapArr: Array[StaOneEleMap] = mkEleMapArr(elehArr)
    //val temEleIdx = getEleHIdx("TEM")
    //val sta_TemEleMap = eleMapArr(temEleIdx)
    //val sta_DPTEleMap = eleMapArr(getEleHIdx("DPT"))
    val egMapArr = Array.ofDim[StaEGMap](numEle4Ground)
    for (eleIdx <- 0 until numEle4Ground) {
      val eleNm: String = EleNms(eleIdx)
      val sta_EleMap: StaOneEleMap = eleMapArr(eleIdx)
      val dictNear: TrieMap[Int, StawdArr] = StaNear.switchNear(eleIdx, 0)
      val trie = TrieMap[Int,EG]()
      sta_EleMap.par.foreach { f =>
        val sta = f._1
        val e = f._2
        val grade: Float = calcu_chk_gradient(eleNm, dictNear(sta), sta, sta_EleMap)
        val eg = if ((eleNm.contains("PRE") || eleNm.contains("VISM")) && grade.isNaN)
          EG(e, Float.NaN)
        else if (grade.isNaN)
          EG(Float.NaN, Float.NaN)
        else if (eleNm.contains("PRS") && (e>=950f) && calcuCnt(dictNear(sta), sta_EleMap, 950f,isGe = true)<2)
          EG(Float.NaN, Float.NaN)
        else if (eleNm.contains("PRE") && (e>=LN20) && calcuCnt(dictNear(sta), sta_EleMap,LN01,isGe = true)<2)
          //雨量 >= 50, 周围无雨，视为缺测。在雨量器鉴定检测时发生
          EG(Float.NaN, Float.NaN)
        else
          EG(e, grade)
        trie +=((sta, eg))
      }
      egMapArr(eleIdx) = trie
    }
    egMapArr
  }

/*
  def nearPreOk(e: Float, sta: Int, dictNear: Map[Int, StawdArr], sta_EleMap: StaOneEleMap): Boolean = {
    val eq = Array(
      //Array(LN20, LN30,  LN01),
      //Array(LN30, LN50,  LN05),
      Array(LN50, LN90,  LN01),
      Array(LN90, LN_Max,LN05)
    )
    val leastCnt =2
    val bs = eq.map { f =>
      val cnt = calcuCnt(dictNear(sta), sta_EleMap, f(2),isGe = true)
      e >= f(0) && e < f(1) && cnt >= leastCnt
    }
    bs.count(f => f) > 0
  }
*/
}
