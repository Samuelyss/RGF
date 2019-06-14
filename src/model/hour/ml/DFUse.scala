package model.hour.ml

import common.BaseType.{AF1, SeqTup3, StaElesMap, StaOneEleMap}
import org.apache.spark.sql.{DataFrame, SparkSession}
import dict.StaDict
import scala.collection.concurrent.TrieMap
import common.SparkCont._
import common.MyConfig._
/**
  * 使用数据集
  * Created by 何险峰，北京 on 16-12-5.
  */
case class DFUse(sampBP  : TSamp,  sampHsd : TSamp) extends DF {
  def mkDf(fitEleIdx: Int, seq0: SeqTup3): DataFrame = {
    if (seq0.isEmpty) null else {
      val seq1 = seq0.map(sta_label_af1 => mkStaLabelFeatureRec(sta_label_af1))
      // must use ss.sparkContext.parallelize(seq1)
      ss.createDataFrame(ss.sparkContext.parallelize(seq1,slice)).toDF("sta", "label", "features")
      // ss.createDataFrame(seq1).toDF("sta", "label", "features")
    }
  }

  def mkLabelsMap(fitEleIdx: Int, sta_labelsMap: StaElesMap): StaOneEleMap = {
    val trie = TrieMap[Int,Float]()
    StaDict.dictStaAll.par.foreach{ f=>
      val sta = f._1
      val label = if (sta_labelsMap != null && sta_labelsMap.keySet(sta)) {
        val af1 :AF1 = sta_labelsMap(sta)
        af1(fitEleIdx)
      } else Float.NaN
      trie +=((sta, label))
    }
    trie
  }
}
