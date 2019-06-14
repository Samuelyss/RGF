package model.hour.ml

import common.BaseType._
import common.MyConfig._
import common.MyUtils.{calcuGradientAlt, logMsg}
import common.SparkCont.slice
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import dict.{StaDict, StaNear}
import org.apache.spark.ml.linalg

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}
/**
  * 为学习，使用数据集提供统一接口和方法
  * Created by 何险峰，北京 on 16-12-5.
  */
trait DF extends TSampImportance {
  val dictV   : StaMap = StaDict.fetchDict(dictVFnm)
  val sampBP  : TSamp
  val sampHsd : TSamp
  val dfs     : Array[DataFrame] = mkDfs
  def mkLabelsMap(fitEleIdx: Int, sta_labelsMap: StaElesMap): StaOneEleMap
  def mkDf(fitEleIdx: Int, seq0: SeqTup3): DataFrame

  def datNm: String =  if (hsd_ftpDir == "none")
    "pure_ground"
  else if (sampHsd.sta_af1_stand == null ) {
    "ground"
  } else
    "hsd"

  private def mkDfs : Array[DataFrame]={
    /**
      * 返回预报对象
      */
    def mkSta_LabelsMap: StaElesMap = {
      val sta_LabelMap = sampBP.sta_af1_src.map { f =>
        val fits = Array.ofDim[Float](nFit)
        val sta = f._1
        val af1 = f._2
        for (i <- 0 until nFit) {
          val eleNm = fitEleNms(i)
          val eleIdx = getEleHIdx(eleNm)
          fits(i) = af1(eleIdx)
        }
        sta -> fits
      }
      sta_LabelMap
    }
    val dfs = Array.ofDim[DataFrame](nFit)
    val sta_LabelsMap = mkSta_LabelsMap
    val mayFail =  if (sta_LabelsMap != null) Try{
      for (i <- 0 until nFit) {
        val eleNm = fitEleNms(i)
        val sta_af1_1 = mkFeatureAF1(eleNm)
        /*
          datNm match {
          case "ground" | "pure_ground" => sampBP.joinAF1_grade(eleNm)
          case "hsd"                    => mkFeatureAF1(eleNm)
        }
        */
        val seq0 = mkSeq_staLabAf1(sampBP.ymdhm,i, sta_LabelsMap, sta_af1_1)
        dfs(i) = mkDf(i,seq0)
      }
    }
    val clsNm = this.getClass.getName
    mayFail match {
      case Success(v) =>
        val msg = s"$clsNm, ${sampBP.ymdhm},$datNm 建立样本集Success $v"
        logMsg(sampBP.ymdhm, "6","2", "I", s"${sampBP.ymdhm}# $msg")
      case Failure(e) =>
        val msg = s"$clsNm, ${sampBP.ymdhm},$datNm 建立起不完全成功样本集. $e"
        logMsg(sampBP.ymdhm, "6","2", "F", s"${sampBP.ymdhm}# $msg")
        //throw new Exception(msg)
    }
    dfs
  }

  /**
    * 建立站点，标签，特征三元序列
    */
  private def mkSeq_staLabAf1(ymdhm : String,
                              fitEleIdx: Int,
                              sta_labelsMap: StaElesMap,
                              sta_af1Map: StaElesMap
                             ): SeqTup3 = {
    val eleNm = fitEleNms(fitEleIdx)
    val buf = new ListBuffer[Tup3]()
    //println(s"mkSeq3meta,${fitEleNms(fitEleIdx)}")
    try {
      // 用于标签
      val sta_label_map: StaOneEleMap = mkLabelsMap(fitEleIdx, sta_labelsMap)
      if (sta_label_map.isEmpty) buf else {
        val sta_af1_feture = sta_af1Map.filter(f => sta_label_map.keySet(f._1))
        val sta_labele_features_seq = mkSta_labele_features_seq(sta_label_map, sta_af1_feture)
        buf.appendAll(sta_labele_features_seq)
      }
      //println(s"eleNm=$eleNm,buf.size=${buf.size}")
    } catch {
      case e: Exception =>
        val msg = s"$eleNm, 建立站点，标签，特征三元序列失败!，buf.size = ${buf.size},${e.toString}"
        logMsg(sampBP.ymdhm, "6","1", "F", s"${sampBP.ymdhm}# $msg")
      // throw new Exception(msg )
      // buf
    }
    buf
  }

  private def mkFeatureAF1(eleNm : String ):StaElesMap={
    val elehs : Array[String] = eleNm match {
      // 201607080000尼伯特台风表明：
      // 对降水不能够将气温、地温GST作为Feature加入，否则对 大陆未降水的台风 失效。
      case "PRE_1h"  =>
        Array("TEM","GST","RHU","DPT","VAP","U","V","QSE", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "PRE_10m" | "PRE_5mpost" | "PRE_5mprev"   =>
        Array("TEM","GST","RHU","DPT","VAP","U","V","QSE","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "VISM" =>
        Array("TEM", "GST","RHU","DPT","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "TEM" =>
        Array("GST","RHU","DPT","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "GST" | "T_5cm"| "T_10cm"| "T_15cm"| "T_20cm"| "T_40cm" =>
        Array("TEM","RHU","DPT","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "RHU" =>
        Array("TEM","GST","DPT","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "DPT" =>
        Array("TEM","GST","RHU","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "VAP" =>
        Array("TEM","GST","RHU","DPT","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case "U" | "V" =>
        Array("TEM","GST","RHU","DPT","QSE","VAP","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi","FFMAX")
      case "WIN_S_Avg_2mi" =>
        Array("GST","RHU","DPT","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "FFMAX")
      case  "FFMAX"=>
        Array("GST","RHU","DPT","VAP","QSE","U","V","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
      case _ =>
        Array("TEM","GST","RHU","DPT","QSE","U","V","VAP","PRE_1h", "WIN_D_Avg_2mi", "WIN_S_Avg_2mi")
    }
    val len = elehs.length
    val ielehs = Array.tabulate[Int](len)(i => getEleHIdx(elehs(i)))
    joinAF1(eleNm,ielehs)
  }

  private def joinAF1(eleNm:String,ielehs:AI1 ): StaElesMap = {
    val dictNear = StaNear.fetchStaNear(dictAFnm)
    val trie = TrieMap[Int,AF1]()
    val hasHsd = datNm == "hsd" && sampHsd.sta_af1_stand !=null
    val hasEle = sampBP.sta_af1_stand !=null
    if (hasEle) StaDict.dictStaAll.par.foreach { s =>
      val sta = s._1
      val info = s._2
      val grade = Array[Float](calcuGradientAlt(eleNm,dictNear(sta), info.alt))
      val af1_hsd = if (hasHsd) sampHsd.sta_af1_stand(sta) else null
      val alt = Array[Float](info.alt)

      val af1_eleh = sampBP.sta_af1_stand(sta)
      val af1_es = ielehs.map(i => af1_eleh(i))
      val af1_gs = ielehs.map(i => af1_eleh(i + numEle4Ground))
      //不能够把alt0加入到fetures中，否则会把西部的高地天气应用到东部台湾
      //不能够把lat,lon加入到fetures中，否则会产生条带形变
      //不同的预报对象添加不同的参数
      val af1 =
      if (! hasHsd)
        af1_es ++ grade
      else if (eleNm.contains("PRE") && hasHsd)
        af1_hsd  ++ grade
      else
        af1_hsd ++ af1_es ++ af1_gs ++ grade

      val af2 = if (eleNm.contains("PRE"))
        af1
      else
        af1 ++ alt
      trie += ((sta,af2))
    }
    trie
  }

  /**
    * 产生站点，标签，特征元组记录
    */
  protected def mkStaLabelFeatureRec(sta_label_af1: Tup3): (Int, Double, linalg.Vector) = {
    val sta = sta_label_af1._1
    val label = sta_label_af1._2
    val af1 = sta_label_af1._3
    val nFetures = af1.length
    val (values, indices) = af1
      .zipWithIndex
      .filter { f =>
        val v = f._1
        !(math.abs(v) < 1e-5 || v.isNaN)
      }
      .unzip
    val fe = Vectors.sparse(nFetures, indices, values.map(_.toDouble))
    (sta, label, fe)
  }
  /**
    * 创建站点，标签，特征三元组序列
    */
  private def mkSta_labele_features_seq(sta_labelMap: StaOneEleMap, sta_af1: StaElesMap) = {
    def staMod(sta: Int): (Int, Int) = {
      if (sta < ImportK) (sta, sta) else (sta % ImportK, sta)
    }
    val sta_labele_features_seq = sta_labelMap
      .map { f =>
        val (sta, staBig) = staMod(f._1)
        val label = f._2.toDouble
        val af1 = sta_af1(sta)
        (staBig, label, af1)
      }
      .filter(f => f._3 != null)
      .toSeq
    sta_labele_features_seq
  }
}
