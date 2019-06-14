package model.hour.ml

import common.BaseType.{AF1, StaElesMap}
import common.MyConfig._
import common.MyUtils.calcuGradientAlt
import dict.{StaDict, StaNear}
import station.EleHBP.numEle4Ground

import scala.collection.concurrent.TrieMap
/**
  * 为地面气象信任传播样本，融合信任传播样本，卫星样本，提供统一接口
  * Created by 何险峰,北京 on 16-12-5.
  */
trait TSamp {
  val sampnm             : String = this.getClass.getName
  val ymdhm              : String
  val dSrc               : String
  val nFeture0           : Int = sampnm match {
    case "SampBP"  => numEle4Ground
    case "SampHsd" => 16
    case _         => numEle4Ground
  }

  private val sta_af1_src_arr : Array[StaElesMap]=getSta_af1
  //待动力花，标准化数据
  val sta_af1_src        : StaElesMap = sta_af1_src_arr(0) // 原始值
  //地面实况数据
  val sta_af1_src_real   : StaElesMap = if (sta_af1_src_arr.length>1) sta_af1_src_arr(1) else null
  val sta_af1_stand      : StaElesMap = mkSta_af1Stand     // 标准值
  def getSta_af1         : Array[StaElesMap]


  private def mkSta_af1Stand: StaElesMap = {
    if (sta_af1_src != null) {
      val vals: Array[AF1] = sta_af1_src
        .values
        .toArray
        .transpose
        .map(f => stat.Statistics.toStand(f))
        .transpose
      val stas = sta_af1_src.keySet.toArray
      val sta_vals = stas.zip(vals).toMap
      val trie = TrieMap[Int,AF1]()
      StaDict.dictStaAll.par.foreach{ s =>
        val sta = s._1
        val af1 = sta_vals(sta)
        trie +=((sta, af1))
      }
      trie
    } else null
  }

  def joinAF1_grade(eleNm : String): StaElesMap = {
    val dictNear = StaNear.fetchStaNear(dictAFnm)
    val trie = TrieMap[Int,AF1]()
    StaDict.dictStaAll.par.foreach{ s =>
      val sta = s._1
      val info = s._2
      val alt0 = info.alt
      val grade = Array[Float](calcuGradientAlt(eleNm,dictNear(sta), alt0))
      val af1 = sta_af1_stand(sta) ++ grade
      trie += ((sta, af1))
    }
    trie
  }
}
