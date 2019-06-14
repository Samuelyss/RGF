package stat

import java.io.File

import common.BaseType
import common.BaseType.{LLPair, StaOneEleMap, StawdArr}
import common.MyConfig._
import common.MyUtils._

import scala.math.{cos, toRadians}
import dict.{StaDict, StaNear}

import scala.collection.concurrent.TrieMap
/**
  * 对甘肃张圆圆的数据进行统计分析（水平平流项）计算各要素平流递减率
  * Created by 何险峰，北京 on 16-12-30.
  */
object AdvRate {
  val nearA: TrieMap[Int, BaseType.StawdArr] = StaNear.fetchStaNear(dictAFnm)
  val root = "/wk2/zyy"
  val srcDir = s"$root/tFile"
  val dstDir = s"$root/h_res"
  val enms = Array("DPT","GST","PRS","QSE","RHU","TEM","U","V","VISM","VAP","WS","T_5cm","T_40cm","PRE_1h")
  val ms: Array[String] = Range(1,13).toArray.map(f => f.formatted("%02d"))

  def main(args: Array[String]): Unit = {
    //mkZRates("DPT", "01")

    ms.foreach { m =>
      print(m + "-->")
      enms.foreach { enm =>
        print(enm + ",")
        mkAdvRates(enm, m)
      }
      println()
    }
  }
  def mkAdvRates(enm : String,m : String): Unit ={
    val sta_speed : StaOneEleMap = readMean("WS",m)
    //enm:要素名, m:月份
    val enmDir = s"$dstDir/$enm"
    new File(enmDir).mkdirs()
    val dstFnm = s"$enmDir/2016$m.txt"
    if (!new File(dstFnm).exists()){
      val sta_v = readMean(enm,m)
      val sta_r = staAdvRates(enm,sta_v,sta_speed)
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
        trie += ((sta, mean))
      }
    trie
  }
  def calcuDistSq(ll0: LLPair)(ll1: LLPair): Float = {
    val dx = (ll0._2 - ll1._2) * cos(toRadians(ll0._1)).toFloat
    val dy = ll0._1 - ll1._1
    val sq = sqr(dx) + sqr(dy)
    sq
  }

  def staAdvRates(eleNm:String,sta_v : StaOneEleMap, sta_speed : StaOneEleMap):StaOneEleMap={
    val scale = 1000f / 111000f
    val trie = TrieMap[Int,Float]()
    sta_v.par.foreach{ f=>
      val sta = f._1
      val v0 = f._2
      val speed = sta_speed(sta)
      val s0 = StaDict.dictStaAll.getOrElse(sta,null)
      if (s0 != null) {
        val ll0 : LLPair = (s0.lat,s0.lon)
        val nes: StawdArr = nearA(sta)
        val ca = calcuDistSq(ll0) _
        val gs = nes.map { f =>
          val v1 = sta_v(f.sta)
          val s1 = StaDict.dictStaAll(f.sta)
          val ll1 : LLPair = (s1.lat,s1.lon)
          val d2 = ca(ll1)
          val d = math.sqrt(d2).toFloat
          val g = speed * gradient(eleNm,v0, v1, d) * scale
          g
        }.filter(f => !f.isNaN)

        val sg = gs.sum
        val sglen = gs.length
        val gm = if (sglen > 0 ) sg / sglen else Float.NaN
        trie +=((sta, gm))
      }
    }
    trie
  }
}
