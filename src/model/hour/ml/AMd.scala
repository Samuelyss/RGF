package model.hour.ml

import common.BaseType.{AF1, StaOneEleMap}
import common.MyConfig.{fitEleNms, hsd_ftpDir, nFit}
import common.MyUtils.logMsg
import common.SparkCont
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import dict.StaDict
import station.EleChk._
import station.EleHOp

import scala.collection.concurrent.TrieMap
import common.SparkCont._

/**
  * 机器学习(随机森林)，集成模型。
  * 1. SampBP  -- 信任传播输入样本。不仅是标签的依据，也是特征的子集
  * 2. SampHsd -- 葵花卫星输入样本。作为主要特征的子集
  * 3. DFLearn -- 学习数据集。
  * 4. DFUse   -- 使用数据集。
  * Created by 何险峰，北京 on 16-12-6.
  */
case class Md(ymdhm: String,dSrc: String) extends AMd

trait AMd extends EleHOp {
  val ymdhm: String
  val dSrc: String
  println(s"开始$ymdhm 地面实况处理......")
  sparkReset
  private val init_sampbp = SampBP(ymdhm,dSrc)

  private val sampHsd = if (hsd_ftpDir != "none") {
    println(s"开始$ymdhm 卫星数据处理......")
    SampHsd(ymdhm,"hsd")
  } else null

  val sampBP: TSamp = iter(init_sampbp)

  private def iter( old_sampBP: TSamp): TSamp = {
    val dfs4Learn = DFLearn(old_sampBP, sampHsd)
    val dfs4Use = DFUse(old_sampBP, sampHsd)
    val newSampBP = bind(old_sampBP, dfs4Learn.dfs, dfs4Use.dfs)
    newSampBP
  }

  private def bind(sampBP: TSamp, dfs4Learn: Array[DataFrame], dfs4Use: Array[DataFrame]): SampBP_Fusion = {
    import sampBP._
    val fits: Array[StaOneEleMap] = mkMdAndFits(sampBP, dfs4Learn, dfs4Use)
    val trie = TrieMap[Int, AF1]()
    StaDict.dictStaAll.par.foreach { f =>
      val sta = f._1
      val af1 = if (sta_af1_src != null && sta_af1_src.keySet(sta))
        sta_af1_src(sta).clone
      else
        Array.ofDim[Float](numEle4Ground)
      for (i <- 0 until nFit) {
        val eleIdx = getEleHIdx(fitEleNms(i))
        af1(eleIdx) = if (fits(i).keySet(sta))
          fits(i)(sta)
        else
          Float.NaN
      }
      trie += ((sta, af1))
    }
    SampBP_Fusion(sampBP.ymdhm, trie, sampBP,sampBP.dSrc)
  }

  private def mkMdAndFits(sampBP: TSamp, dfs4Learn: Array[DataFrame], dfs4Use: Array[DataFrame]): Array[StaOneEleMap] = {
    val t0 = System.currentTimeMillis()
    val eleh_fitArr: Array[StaOneEleMap] = Array.ofDim[StaOneEleMap](nFit)
    val msg = s"正在学习/融合 $nFit 要素卫星融合模型...... "
    logMsg(ymdhm, "6", "2", "I", s"$ymdhm# $msg")

    //Range(0,nFit).par.foreach

    def sub(i:Int)= {
      val tt0 = System.currentTimeMillis()
      val eleNm = fitEleNms(i)
      /*
      if (eleNm.contains("TEM"))
        ss.sparkContext.setLocalProperty("spark.scheduler.pool", "tem")
      else
        ss.sparkContext.setLocalProperty("spark.scheduler.pool", "other")
        */
      val aLearnDf: DataFrame = dfs4Learn(i)
      val aUseDf: DataFrame = dfs4Use(i)
      val trie = if (aLearnDf != null) {
        val res = mkModel(i, aLearnDf, aUseDf)
//        dfs4Learn(i) = null
//        dfs4Use(i) = null
        res
      } else {
        val msgFail = s"对$eleNm 无法完成建模"
        logMsg(ymdhm, "6", "2", "F", s"$ymdhm# $msgFail")
        TrieMap[Int, Float]()
      }
      eleh_fitArr(i) = trie

      val dt = (System.currentTimeMillis() - tt0) / 1000
      println(s"$eleNm 融合花费$dt 秒")
    }

    //sub(0)
    Range(0, nFit).par.foreach(sub)

    val dt = (System.currentTimeMillis() - t0) / 1000
    val msgok = s"融合花费$dt 秒. "
    logMsg(ymdhm, "6", "2", "O", s"$ymdhm# $msgok")
    eleh_fitArr
  }

  private def mkModel(fitEleIdx: Int, aLearnDf: DataFrame, aUseDf: DataFrame): StaOneEleMap = {
    val eleNm = fitEleNms(fitEleIdx)
    val tt0 = System.currentTimeMillis()
    val pipeline = new Pipeline().setStages(Array(Model.featureIndexer.fit(aUseDf), Model.rf))
    val model: PipelineModel = pipeline.fit(aLearnDf)
    val dt = (System.currentTimeMillis() - tt0) / 1000
    println(s"$eleNm 训练花费$dt 秒")
    if (model != null) {
      val tt0 = System.currentTimeMillis()
      val predict = mkPredict(aUseDf, model, eleNm)
      val dt = (System.currentTimeMillis() - tt0) / 1000
      println(s"$eleNm 预测花费$dt 秒")
      predict
    }
    else
      TrieMap[Int, Float]()
  }


  /**
    * 根据df提供的labeledPoints建立模型
    */
  private def mkPredict(aUseDf: DataFrame, model: PipelineModel, eleNm: String): StaOneEleMap = {
    val dict = StaDict.switchDict(eleNm).toMap //test add .toMap
    val yfits = model.transform(aUseDf)

    def finePredictPRE(predict: Float, label: Float): Float = {
      if (predict > LN0 || label > LN0)
        math.max(predict, label) + LN01
      else
        predict
    }

    def fineLabelPRE(label: Float, predict: Float): Float = {
      if (label <= LN0 && predict > LN0)
        predict + LN01
      else
        label //math.max(predict,label)
    }

    def fus(sta: Int, label: Float, predict: Float): (Int, Float) = {
      val isVirt = !dict.keySet(sta) // 如果站号不在字典中，则是一个虚拟站

      //对雨量修正预报值
      val predictExt = if (eleNm.contains("PRE"))
        finePredictPRE(predict, label)
      else
        predict //其它，不变

      //对雨量修正标签值
      val labelExt = if (eleNm.contains("PRE")) {
        if (predict <= LN0 && label >= LN20) // 老鼠啃电线)
          LN0
        else
          fineLabelPRE(label, predict)
      } else label

      val v0 = if (labelExt.isNaN) { //1：缺测站，用预报值
        if (eleNm.contains("VISM") && predict >= LN17)
          LN20
        else
          predictExt
      } else if (isVirt) {
        if (eleNm.contains("VISM") && predict >= LN10)
          math.max(label, predict)
        else if (eleNm.contains("PRE"))
          math.max(predictExt, labelExt)
        else
          predictExt
      } else
        labelExt
      (sta, v0)
    }

    //预测
    val trie = TrieMap[Int, Float]()
    yfits.select("sta", "label", "prediction")
      .collect
      .map(r => (r.getAs[Int]("sta"), r.getAs[Double]("label"), r.getAs[Double]("prediction"))) //r是指行,取所有行中"prediction"字段
      .par
      .foreach { f =>
        val sta = f._1
        val label = f._2.toFloat
        val predict = f._3.toFloat
        val v = fus(sta, label, predict)
        trie += v
      }
    trie

    /*
    val trie = TrieMap[Int, Float]()
    yfits.select("sta", "label", "prediction")
      .map(r => {
        val sta = r.getAs[Int]("sta")
        val label = r.getAs[Double]("label").toFloat
        val predict = r.getAs[Double]("prediction").toFloat
        val v = fus(sta, label, predict)
        v
      }).collect()
      .foreach(v => {
        trie += v
      })
    trie
    */
  }
}
