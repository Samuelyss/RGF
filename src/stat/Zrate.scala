package stat

import java.io.File

import common.BaseType
import common.BaseType.{StaOneEleMap, StaWDMap, StawdArr}
import common.MyConfig.dictAFnm
import common.MyUtils._
import dict.{StaDict, StaNear}

import scala.collection.concurrent.TrieMap
/**
  * 对甘肃张圆圆的数据进行统计分析，计算各要素垂直递减率
  * Created by 何险峰，北京 on 16-12-30.
  */
object Zrate {
  val nearA: StaWDMap = StaNear.fetchStaNear(dictAFnm)
  val root = "/wk2/zyy"
  val srcDir = s"$root/tFile"
  val dstDir = s"$root/res"
  val enms: Array[String] = Array("DPT","GST","PRS","QSE","RHU","TEM","U","V","VISM","VAP","WS","T_5cm","T_40cm","PRE_1h")
  val ms: Array[String] = Range(1,13).toArray.map(f => f.formatted("%02d"))

  def main(args: Array[String]): Unit = {
    //mkZRates("DPT", "01")

    ms.foreach { m =>
      print(m + "-->")
      enms.foreach { enm =>
        print(enm + ",")
        mkZRates(enm, m)
      }
      println()
    }
  }
  def mkZRates(enm : String,m : String): Unit ={
    val enmDir = s"$dstDir/$enm"
    new File(enmDir).mkdirs()
    val dstFnm = s"$enmDir/2016$m.txt"
    if (!new File(dstFnm).exists()){
      val sta_v = readMean(enm,m)
      val sta_r = staZRates(enm,sta_v)
      val txt = sta_v2txt(sta_r)
      wrt2txt(dstFnm, txt)
    }
  }

  def sta_v2txt(sta_v: StaOneEleMap): String = {
    sta_v.map { f =>
      val sta = f._1
      val v   = f._2
      val info = StaDict.dictStaAll(sta)
      s"$sta,${info.lat},${info.lon},${info.alt},$v"
    }.mkString("\n")
  }

  def readMean(enm : String,m : String):StaOneEleMap = {
    val trie = TrieMap[Int,Float]()
    val fnm = s"$srcDir/$enm/2016$m.txt"
    fileLines(fnm)
      .tail
      .map(f => f.split(','))
      .map{f =>
        val sta = f(0).toInt
        val mean = f(1).toFloat
        trie += ((sta , mean))
      }
    trie
  }

  def staZRates(eleNm:String,sta_v : StaOneEleMap):StaOneEleMap={
    val sta_zrates = sta_v.map{ f=>
      val sta = f._1
      val a = f._2
      val s0: BaseType.StaInfo = StaDict.dictStaAll.getOrElse(sta,null)
      if (s0 != null) {
        val nes: StawdArr = nearA(sta)
        val gs = nes.filter(f => math.abs(f.dz)>10f ).map { f =>
          val b = sta_v(f.sta)
          val g = gradient(eleNm,a, b, f.dz)
          g
        }.filter(f => !f.isNaN)
        val sg = gs.sum
        val sglen = gs.length
        val gm = if (sglen > 0 ) (sg / sglen) * 100f else Float.NaN
        sta -> gm
      } else sta -> Float.NaN
    }
    sta_zrates.filter(f => !f._2.isNaN )
  }
}
