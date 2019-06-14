package station

import common.BaseType._
import EleChk._
import common.MyConfig._
import common.MyUtils._
import stat.Advect
import dict.{StaDict, StaNear}
import station.cmiss.CmBind

import scala.collection.concurrent.TrieMap

/**
  * 站点信任传播(Belief Propagation)插值
  * Created by 何险峰，北京 on 16-1-19.
  */
case class EleHBP(sta_elesBP: StaElesMap, sta_elesReal: StaElesMap)

object EleHBP extends EleHGradient {
  //private val speedIdx = getEleHIdx("WIN_S_Avg_2mi")
  //private var elehbp_buf: YmdhmEGMapArr = ("", null)

  def main(args: Array[String]): Unit = {
    //mkDicts
    //test0
  }

  def test0(): Unit = {
    val ymdhm = "201609070410"
    val elehBP = this (ymdhm, "cmis")
    println(s"站点记录数：${elehBP.sta_elesBP.size}")
    val eleIdx = getEleHIdx("PRE_1h")
    val sta1 = 887009
    // 5.6
    val v: Float = elehBP.sta_elesBP(sta1)(eleIdx)
    val v1: Float = releasCtl(v, "PRE_1h")
    println(s"$v,$v1")
  }

  def apply(ymdhm: String, dSrc: String): EleHBP = {
    val adv = CmBind(ymdhm, dSrc)
    this (adv,dSrc)
  }

  def apply(adv: Advect, dSrc: String): EleHBP = {
    try {
      val sta_elesReal = arrStaEg2staAf1(adv.real, numEle4Ground)
      StatOutput.mkCsvReal(adv.ymdhm, sta_elesReal,dSrc)
      val recLen = sta_elesReal.size
      if (recLen < 1000) {
        val msg = s"可使用样本数:${recLen}太少，本次处理失败. 请等待... "
        logMsg(adv.ymdhm, "5", "2", "F", s"${adv.ymdhm}# $msg")
        sys.error(msg)
      }
      val sta_elesBP = this (adv.ymdhm, adv)
      EleHBP(sta_elesBP, sta_elesReal)
    } catch {
      case _: Exception =>
        val msg = s"arrStaEg2staAf1 fail. "
        logMsg(adv.ymdhm, "5", "2", "F", s"${adv.ymdhm}# $msg")
        sys.error(msg)
    }
  }

  def apply(ymdhm: String, adv: Advect, nIter: Int = numMsgIter): StaElesMap = {
    val t0 = System.currentTimeMillis()
    val sta_elesBP = if (adv.mend != null && adv.fore != null) {
      val egMapArr1: Array[StaEGMap] = iter(ymdhm, adv, nIter)
      arrStaEg2staAf1(egMapArr1, numEle4Ground)
    } else {
      val msg = "EleHBeliefPropagation.apply:egMapArr0 == null. "
      logMsg(ymdhm, "5", "2", "F", s"$ymdhm# $msg")
      sys.error(msg)
    }
    val dt = (System.currentTimeMillis() - t0) / 1000
    val msg = s"信任传播花费${dt}秒. "
    logMsg(ymdhm, "5", "2", "O", s"$ymdhm# $msg")
    sta_elesBP
  }

  def iter(ymdhm: String, adv: Advect, nIter: Int): Array[StaEGMap] = {
    val msg = s"正在通过$nIter 次迭代，完成站点资料对齐. "
    logMsg(ymdhm, "5", "2", "I", s"$ymdhm# $msg")

    val nAll = StaDict.dictStaAll.size
    for (i <- 0 until nIter) {
      //val sta_eg_speed = adv.fore(speedIdx)
      for (eleIdx <- 0 until numEle4Ground) {
        val eg_mend = adv.mend(eleIdx)
        val eg_fore = adv.fore(eleIdx)
        val cntAbsent = nAll - eg_mend.map(f => f._2.ev).count(f => !f.isNaN)
        if (cntAbsent > 0) {
          val trie = TrieMap[Int, EG]()
          StaDict.dictStaAll.par.foreach{ s =>
            val sta = s._1
            val eg = if (eg_mend.keySet(sta) && !eg_mend(sta).ev.isNaN)
              eg_mend(sta)
            else {
              infer(i, sta, eleIdx, eg_mend, eg_fore)
            }
            trie += ((sta,eg))
          }
          adv.mend.update(eleIdx, trie)
        }
      }
    }
    adv.mend
  }

  /**
    * 对站点sta，要素枚举enum, 站点要素枚举enum对应数据源，插值
    *
    * @param sta    ： 站点号
    * @param eleIdx ： 要素枚举
    * @param src    ： 数据源
    * @return ： 插值后的值
    */
  def infer(nIter: Int, sta: Int, eleIdx: Int, src: StaEGMap, fore: StaEGMap): EG = {
    val ne = StaNear
      .switchNear(eleIdx, nIter)(sta)
      .filter(f => src.keySet(f.sta))
    val eg0 = if (fore==null)
      null
    else
      fore.getOrElse(sta, null)

    val wvas = ne.map { m =>
      val near_eg = src(m.sta)
      Wva(m.w, near_eg, m.dz, m.w * near_eg.ev, eg0, m.d)
    }.filter(f => !f.wv.isNaN)
    val eg = actAlt(eleIdx, wvas,isGrid = false)
    eg
  }
}
