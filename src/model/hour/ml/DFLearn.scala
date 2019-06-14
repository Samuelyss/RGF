package model.hour.ml

import common.BaseType._
import common.MyConfig.{fitEleNms, nFit}
import station.EleChk._
import org.apache.spark.sql.{DataFrame, SparkSession}
import dict.StaDict
import stat.Statistics
import common.MyUtils.logMsg
import common.TimeDFLearnBuf
import scala.collection.concurrent.TrieMap
/**
  * 通过重要性采样，建立学习数据集
  * Created by 何险峰，北京 on 16-12-5.
  */
case class DFLearn(sampBP: TSamp, sampHsd: TSamp) extends DF {
  import common.SparkCont._
  private def mkHistSeq(ymdhm: String, fitEleIdx: Int, seq0: SeqTup3): SeqTup3 = {
    val elenm = fitEleNms(fitEleIdx)
    datNm match {
      case "hsd" | "pure_ground" =>
        val buf = TimeDFLearnBuf
        val arrSeqTup3Buf = buf.refresh(ymdhm, fitEleIdx, seq0)
        //表示本次学习有卫星数据
        buf.keep(ymdhm.toLong, arrSeqTup3Buf)
        val msg = s"$elenm: 学前采样,使用${buf.cach.size}时次数据。 "
        logMsg(ymdhm, "6", "2", "I", s"$ymdhm# $msg")
        var seq1: SeqTup3 = buf.vals.head(fitEleIdx)
        for (i <- 1 until buf.vals.size) {
          seq1 ++= buf.vals(i)(fitEleIdx)
        }
        seq1
      case "ground" =>
        //表示本次学习仅有地面数据
        val msg = s"$elenm:学前采样使用1时次地面数据。 "
        logMsg(ymdhm, "6", "2", "I", s"$ymdhm# $msg")
        seq0
    }
  }

  def mkDf(fitEleIdx: Int, seq0: SeqTup3): DataFrame = {
    if (seq0.isEmpty) null else {
      val seq1 = mkHistSeq(sampBP.ymdhm, fitEleIdx, seq0)
      val seq2 = seq1.map(sta_label_af1 => mkStaLabelFeatureRec(sta_label_af1))
      // must use ss.sparkContext.parallelize(seq1)
      ss.createDataFrame(ss.sparkContext.parallelize(seq2,slice))
        .toDF("sta", "label", "features")
      // ss.createDataFrame(seq1).toDF("sta", "label", "features")
    }
  }

  def mkLabelsMap(fitEleIdx: Int, sta_labelsMap: StaElesMap): StaOneEleMap = {
    val eleNm = fitEleNms(fitEleIdx)
    val eleIdx = getEleHIdx(eleNm)
    val staDict = StaDict.switchDict(eleIdx)

    val trie = TrieMap[Int, Float]()
    sta_labelsMap.filter(f => staDict.keySet(f._1)).par.foreach { f =>
      val sta = f._1
      val label = f._2(fitEleIdx)
      if (!label.isNaN) trie += ((sta, label))
    }

    val sta_label_map1: StaOneEleMap =
      if (eleNm.contains("PRE")) trie.filter { f =>
        val sta = f._1
        val sta_info = StaDict.dictStaAll(sta)
        val lat = sta_info.lat
        val lon = sta_info.lon
        val label = f._2
        //val isWest = lon < 106f || lat > 35f
        val isWest = lon < 104f || lat > 35f
        val east_law = !isWest && label > LN0 && label <= LN70
        val west_law = isWest && label >= LN0 && label <= LN60
        east_law || west_law
      } else trie

    if (sta_labelsMap.isEmpty || sta_label_map1.isEmpty || sta_label_map1.size < minNumSamp)
      sta_label_map1
    else if (eleNm.contains("PRE")) {
      val es = sta_label_map1.values.toArray
      val statis = Statistics(es)
      val ext = extSamp(sta_label_map1, statis, '>', numRepeat)
      sta_label_map1 ++= ext
    } else
      sta_label_map1
  }
}
