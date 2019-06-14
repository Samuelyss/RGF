package grid

import java.io.File
import common.BaseType._
import common.MyConfig._
import common.MyUtils._
import common.TimeTransform._
import dict.StaDict
import model.hour.ml.Md
import station.{EleHBP, EleHOp, StatOutput}

/**
  * 将10万个离散站点资料格点化
  * Created by 何险峰，北京 on 16-2-20.
  */
case class GdEleH(ymdhm: String, as3: AS3, sta_elesFine: StaElesMap, sta_elesReal: StaElesMap)

object GdEleH extends EleHOp with Gd {
  //var curGdEleH : GdEleH = null
  def main(args: Array[String]): Unit = {
    //test1
  }

  def test1(): Unit = {
    val t0 = System.currentTimeMillis()
    val ymdh0 = "201711080000"
    val isFore=false
    val dataSrc = "cmis"
    GdEleH(ymdh0, isFore, dataSrc)
    val dt = (System.currentTimeMillis() - t0) / 1000 / 60
    println(ymdh0, "INFO", "OK", s"takes : $dt m")
  }

  def apply(ymdhm: String, isFore : Boolean,dSrc: String = "cmis"): GdEleH = {
     if (option>1) {
       learn(ymdhm,isFore, dSrc)
     } else {
       val eleHBP = EleHBP(ymdhm, dSrc)
       val sta_elesFine = staAf12sta_elesMap(eleHBP.sta_elesBP)
       val as3 = GdEleH(ymdhm, isFore, sta_elesFine)
       val gdEleH = GdEleH(ymdhm, as3, sta_elesFine, eleHBP.sta_elesReal)
       mkStatTxt(ymdhm, gdEleH)
       gdEleH
    }
  }

  def apply(ymdh: String, isFore : Boolean,sta_elesFine: StaElesMap): AS3 = {
    val t0 = System.currentTimeMillis()
    mkInterpolate(ymdh, isFore,sta_elesFine)
  }

  /**
    * Only for csv0 reserve
    */
  def apply4sta_csv0(ymdhm: String,dSrc: String)={
    val (csv0fnm,_)=getCsvFnms(ymdhm)
    val (_,hsdExisted) = grid.hsd.HsdHxfGd.chkYmdhmFileExists(ymdhm)
    if (!(new File(csv0fnm).exists()) || ! hsdExisted) {
      val (sta_elesFine, _) = mkCsv0(ymdhm, dSrc)
      println(s"写出融合后$csv0fnm")
      saveCsv(ymdhm, csv0fnm, sta_elesFine, StaDict.dictStaAll)
    }
  }

  private def mkCsv0(ymdhm: String,dSrc: String): (StaElesMap,StaElesMap) ={
    println("进入学习模块...")
    val mdResult = new Md(ymdhm,dSrc)
    val sta_elesFine: StaElesMap =
      if (mdResult != null)
        staAf12sta_elesMap(mdResult.sampBP.sta_af1_src)
      else
        staAf12sta_elesMap(EleHBP(ymdhm, dSrc).sta_elesBP)
    (sta_elesFine,mdResult.sampBP.sta_af1_src_real)
  }

  private def learn(ymdhm: String,isFore : Boolean, dSrc: String): GdEleH = {
    val (sta_elesFine,sta_af1_src_real) = mkCsv0(ymdhm,dSrc)
    val as3 = GdEleH(ymdhm, isFore,sta_elesFine)
    val gdEleH = GdEleH(ymdhm,as3, sta_elesFine, sta_af1_src_real)
    mkStatTxt(ymdhm, gdEleH)
    gdEleH
  }

  private def mkStatTxt(ymdhm: String, gdEleH: GdEleH) {
    val fnms = getCsvFnms(ymdhm)
    val csv0Fnm = fnms._1
    val csvfitFnm = fnms._2
    StatOutput(csv0Fnm, csvfitFnm, ymdhm, gdEleH)
  }

  def getCsvFnms(ymdhm: String): (String, String) = {
    val year = getYear(ymdhm)
    val ymd = getYmd(ymdhm)

    val csv0 = s"$csvDir/eleh/csv0/$year/$ymd"
    val csvfit = s"$csvDir/eleh/csvfit/$year/$ymd"

    new File(csv0).mkdirs
    new File(csvfit).mkdirs
    (s"$csv0/$ymdhm.csv", s"$csvfit/$ymdhm.csv")
  }

}
