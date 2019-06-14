package dict

import common.MyConfig._
import station.EleHOp

/**
  * Created by 何险峰，维也纳 on 2017/4/1.
  */
trait Dict extends EleHOp{
  //val state : String   //new,ext,read,
  val iFactor = 10000 //行因子
  val dictNearNms :Array[String]= Array[String](
    dictAFnm,dict6Fnm,dict3Fnm,dictPRSFnm,dictRHUFnm, dictTEMFnm,
    dictWindFnm,dictVISMFnm,dictT0Fnm
    ,dictGridFnm
  )
  val dictNearNmsExt: Array[String] = dictNearNms ++ Array(dictGridExtFnm)

  val dictNms :Array[String] = dictNearNms ++ Array(dictRFnm,dictVFnm)

  //行列号编码
  def ij2z(iLat:Int,jLon:Int ):Int={
    iLat * iFactor + jLon
  }

  implicit class QuotRem(x: Int) {
    def /%(y: Int): (Int, Int) = (x / y, x % y)
  }

  //解码到行列号
  def z2ij(z : Int):(Int,Int)={
    z /% iFactor
  }

  def getDictFnm(fnm: String): String = {
    if (fnm.indexOf("csv") < 0) s"$fnm.csv" else fnm
  }

  def getNearStaWDZFnm(dictFnm: String): String = {
    s"${dictFnm}_knn_wd.csv"
  }

  def switchDictNm(elehNm: String): String = {
    val dictNm  = elehNm match {
      case "PRS" | "PRS_Sea" | "QSE" =>  dictPRSFnm
      case "GST" | "T_5cm" | "T_10cm" | "GTEM"=> dictT0Fnm
      case "T_15cm" | "T_20cm" | "T_40cm"     => dictT0Fnm //dict3Fnm
      case "VISM" =>   dictVISMFnm
      case "TEM" =>    dictTEMFnm
      case "DPT" | "RHU" | "VAP" =>  dictRHUFnm
      case "U" | "V" |
           "DDDMAX" | "FFMAX" |
           "WIN_D_Avg_2mi" | "WIN_S_Avg_2mi" => dictWindFnm
      case "PRE_1h" | "PRE_10m" |
           "PRE_5mprev"| "PRE_5mpost" => dict6Fnm
      case _ => dict3Fnm
    }
    dictNm
  }
  def dictFnm2EleNm(dictFnm : String):String={
    val eleNm = dictFnm match {
      case `dictPRSFnm` => "PRS"
      case `dictRHUFnm` => "RHU"
      case `dictWindFnm` => "WIN_D_Avg_2mi"
      case `dictTEMFnm` => "TEM"
      case `dict6Fnm` => "PRE_1h"
      case `dictVISMFnm` => "VISM"
      case `dictT0Fnm` => "GST"
      case _ => ""
    }
    eleNm
  }

  def switchDictNm(elehIdx: Int): String = {
    val elehNm = EleNms(elehIdx)
    switchDictNm(elehNm)
  }

}
