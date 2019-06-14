package grid

import common.BaseType._
import station.EleChk._
import common.MyUtils._
import common.MyConfig._
import dict.{StaDict, StaNear}
import station.{EleChk, EleHOp, Wva}

import scala.collection.concurrent.TrieMap
/**
  * Created by 何险峰，北京 on 16-6-19.
  */
trait Gd extends EleHOp {
  val gdNear: StaWDMap = StaNear.fetchStaNear(dictGridFnm)
  val gdNearExt: StaWDMap = StaNear.fetchStaNear(dictGridExtFnm)
  val dictGridIJ: TrieMap[Int, (Int, Int)] = StaDict.dictGridIJ

  def mkInterpolate(ymdhm: String, isFore: Boolean, sta_af1: StaElesMap): AS3 = {
    val as3 = Array.fill[Short](numEle4Ground, Dem.nLat, Dem.nLon)(MISSING_SHORT)
    val t0 = System.currentTimeMillis()
    val msg = "开始网格插值..."
    logMsg(ymdhm, "8", "3", "I", s"$ymdhm# $msg")
    val sta_elesMap0 = staAf12sta_elesMap(sta_af1)
    //确保雨量，能见度为 < 0的，进行对数逆变换数值
    val sta_elesFine = releasCtl_sta_eleMapArr(sta_elesMap0)

    Range(0, numEle4Ground).foreach { eleIdx =>
      val eleNm = EleNms(eleIdx)
      val eleFactor = if (eleNm.contains("PRE")) 100 else 10
      mkLatLonMat(as3, isFore, sta_elesFine, eleIdx, eleNm, eleFactor)
    }

    val dt = (System.currentTimeMillis() - t0) / 1000
    val msg1 = s"网格插值花费$dt 秒"
    logMsg(ymdhm, "8", "3", "I", s"$ymdhm# $msg1")
    as3
  }

  private def mkLatLonMat(as3: AS3, isFore: Boolean, sta_elesMap: StaElesMap, eleIdx: Int, eleNm: String, eleFactor: Int): Unit = {
    eleNm match {
      case "PRE_1h" | "PRE_10m" | "PRE_5mprev" | "PRE_5mpost" =>
        //使用max(高通,低通)滤波增强降水效果
        gdNearExt.par.foreach { zwd =>
          val sta_z = zwd._1
          val stawArr: StawdArr = zwd._2
          val (s0, _) = interFlat(stawArr, sta_elesMap, eleIdx, eleFactor)
          val (s1, _) = interFlat(gdNear(sta_z), sta_elesMap, eleIdx, eleFactor)
          val s2: Short = s0 max s1
          setVal(as3, eleIdx, sta_z, s2)
        }

      case "PRS" | "PRS_Sea"
           | "RHU" | "VAP"
           | "WIN_S_Avg_2mi" | "FFMAX" | "VISM" | "U" | "V" | "WIN_D_Avg_2mi" | "DDDMAX" =>
        //使用高通滤波，使图面真实
        //对温度类使用该过滤器，会使rmse达到0.3左右，但用户体验较差
        gdNear.par.foreach { zwd =>
          val sta_z = zwd._1
          val stawArr: StawdArr = zwd._2
          val (s0, n0) = interFlat(stawArr, sta_elesMap, eleIdx, eleFactor)
          val (s1, _) = if (n0 <= 1)
            interFlat(gdNearExt(sta_z), sta_elesMap, eleIdx, eleFactor)
          else
            (s0, n0)
          setVal(as3, eleIdx, sta_z, s1)
        }

      case _ =>
        //使用低通滤波+常量温度递减率，使图面美观
        gdNearExt.par.foreach { zwd =>
          val sta_z = zwd._1
          val stawArrExt: StawdArr = zwd._2
          val stawArr: StawdArr = gdNear(sta_z)
          val (s0, _) = interZRate_const(stawArrExt, stawArr,eleIdx,eleFactor,sta_elesMap)
          setVal(as3, eleIdx, sta_z, s0)
        }
    }
  }

  private def interZRate_const(stawArrExt: StawdArr, stawArr: StawdArr, eleIdx: Int, eleFactor: Int, sta_elesMap: StaElesMap): (Short, Int) = {
    //使用最多的邻近站建立初始值
    val e0 = calcuE(stawArrExt, 0f,sta_elesMap,eleIdx)
    // 迭代后，RMSE下降明显，但视觉效果较差，
    val e1 = if (option == 3) {
      //使用最少邻近站，求变化值
      val de0 = calcuE(stawArr, e0, sta_elesMap, eleIdx)
      if (de0.isNaN) e0 else e0 + 0.3f * de0
    } else e0

    val ev = elehTrim(eleIdx, e1, isGrid = true)
    val s = (ev * eleFactor).toShort
    (s, 5)
  }

  private def calcuE(stawArr: StawdArr, e0: Float, sta_elesMap: StaElesMap, eleIdx: Int): Float = {
    val dwvas = stawArr
      .map { f =>
        val ev = sta_elesMap(f.sta)(eleIdx) - e0
        val w = f.w
        val eg = EG(ev, 0f)
        val dz = f.dz
        val wv = w * ev
        val eg0 = EG(Float.NaN, Float.NaN)
        val d = f.d
        Wva(w, eg, dz, wv, eg0, d)
      }
      .filter(f => f.wv != Float.NaN)
    val deg0: EG = actAlt(eleIdx, dwvas, isGrid = true)
    deg0.ev
  }

  private def interFlat(stawArr: StawdArr, sta_elesMap: StaElesMap, eleIdx: Int, eleFactor: Int): (Short, Int) = {
    val wvs = stawArr
      .map { f =>
        val e = sta_elesMap(f.sta)(eleIdx)
        (f.w, e)
      }
      .filter(f => !f._2.isNaN)
    val numInter = wvs.length
    if (wvs.nonEmpty) {
      val sumw = wvs.map(f => f._1).sum
      val sumwv = wvs.map(f => f._1 * f._2).sum
      val e = sumwv / sumw
      val ev = elehTrim(eleIdx, e, isGrid = true)
      val s = (ev * eleFactor).toShort
      (s, numInter)
    } else (MISSING_SHORT, 0)
  }

  private def setVal(as3: AS3, eleIdx: Int, sta_z: Int, v: Short): Unit = {
    if (v != MISSING_SHORT) {
      val (i, j) = dictGridIJ(sta_z)
      as3(eleIdx)(i)(j) = v
    }
  }
}
