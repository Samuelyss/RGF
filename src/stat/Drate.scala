package stat

import java.io.File

import common.BaseType.{LLPair, StaOneEleMap, StaWDMap, StawdArr}
import common.MyConfig.dictAFnm
import common.MyUtils._

import scala.math.{cos, toRadians}
import dict.{StaDict, StaNear}
import grid.Dem

import scala.collection.concurrent.TrieMap
/**
  * 对甘肃张圆圆的数据进行统计分析（水平）计算各要素水平距离递减率
  * Created by 何险峰，北京  on 17-1-11.
  */
object Drate {
  val nearA: StaWDMap = StaNear.fetchStaNear(dictAFnm)
  val root: String = "/wk2/zyy"
  val srcDir: String = s"$root/tFile"
  val dstDir: String = s"$root/d_res"
  val enms = Array("DPT","GST","PRS","QSE","RHU","TEM","U","V","VISM","VAP","WS","T_5cm","T_40cm","PRE_1h")
  val ms: Array[String] = Range(1,13).toArray.map(f => f.formatted("%02d"))

  def main(args: Array[String]): Unit = {
    //mkZRates("DPT", "01")

    ms.foreach { m =>
      print(m + "-->")
      enms.foreach { enm =>
        print(enm + ",")
        mkDRates(enm, m)
      }
      println()
    }
  }
  def mkDRates(enm : String,m : String): Unit ={
    //enm:要素名, m:月份
    val enmDir = s"$dstDir/$enm"
    new File(enmDir).mkdirs()
    val dstFnm = s"$enmDir/2016$m.txt"
    if (!new File(dstFnm).exists()){
      val sta_v = readMean(enm,m)
      val sta_r = staDRates(enm,sta_v)
      val txt = sta_v2txt(sta_r)
      wrt2txt(dstFnm, txt)
    }
  }

  def sta_v2txt(sta_v: StaOneEleMap): String = {
    sta_v.map { f =>
      val sta = f._1
      val v   = f._2
      val info = StaDict.dictStaAll(sta)
      val vs = v.formatted("%.7f")
      s"$sta,${info.lat},${info.lon},${info.alt},$vs"
    }.mkString("\n")
  }

  def readMean(enm : String,m : String):StaOneEleMap = {
    val fnm = s"$srcDir/$enm/2016$m.txt"
    val trie = TrieMap[Int,Float]()
    fileLines(fnm)
      .tail
      .map(f => f.split(','))
      .foreach{f =>
        val sta = f(0).toInt
        val mean = f(1).toFloat
        trie += ((sta , mean))
      }
    trie
  }
  def calcuDistSq(ll0: LLPair)(ll1: LLPair): Float = {
    val dx = (ll0._2 - ll1._2) * cos(toRadians(ll0._1)).toFloat
    val dy = ll0._1 - ll1._1
    val sq = sqr(dx) + sqr(dy)
    sq
  }

  def staDRates(eleNm:String,sta_v : StaOneEleMap):StaOneEleMap={
    val sta_hrates = sta_v.map{ f=>
      val sta = f._1
      val a = f._2
      val s0 = StaDict.dictStaAll.getOrElse(sta,null)
      if (s0 != null) {
        val ll0 : LLPair = (s0.lat,s0.lon)
        val nes: StawdArr = nearA(sta)
        val ca = calcuDistSq(ll0) _
        val gs = nes.filter(f => f.d > Dem.step).map { f =>
          val b = sta_v(f.sta)
          val g = gradient(eleNm,a, b, f.d)
          g
        }.filter(f => !f.isNaN)
        val sg = gs.sum
        val sglen = gs.length
        val gm = if (sglen > 0 ) sg / sglen else Float.NaN
        sta -> gm
      } else sta -> Float.NaN
    }
    sta_hrates.filter(f => !f._2.isNaN )
  }

}
