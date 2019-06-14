package station.cmiss

import com.alibaba.fastjson.JSONObject
import common.MyConfig._
import common.TimeTransform._
import station.EleChk._

import scala.collection.concurrent.TrieMap

/**
  * Created by 何险峰，汤沛， 北京  on 16-7-9.
  */
object CmERain extends Cmiss[ERain] {
  def main(args: Array[String]) {
    val ymdhm = "201607080000"
    val ymdhm_10 = prevYmdhm_minute(ymdhm,10)
    val map = this (ymdhm_10,ymdhm)
    println(map.size)
  }
  var dt = 0
  def apply(ymdhm_start: String,ymdhm_end: String): TrieMap[Int, ERain] = {
    dt = dtInMinu(ymdhm_end,ymdhm_start)
    val a = getEleXArr("ERain",ymdhm_start,ymdhm_end).toArray
    elex2map(a)
  }
  implicit def toERain(x: ERain): ERain = x.asInstanceOf[ERain]
  def mkURLStr(ymdhm_start: String,ymdhm_end: String): String = {
    require(ymdhm_start.length == 12 && ymdhm_end.length == 12,s"$ymdhm_start to $ymdhm_end error。")
    val m0 = getMinutes(ymdhm_start)
    val m1 = getMinutes(ymdhm_end)
    require(m0 % 5 == 0 && m1 % 5 == 0 )

    val ymdhm0 = ymdhm_start + "00"
    val ymdhm1: String = ymdhm_end   + "00"

    //logMsg(ymdhm_start,"INFO","OK",s"Rain : ${ymdhm0}  -- ${ymdhm1}")
    val URLMinRain = cmiss_http +
      "&interfaceId=statSurfEle&dataCode=SURF_CHN_PRE_MIN" +
      "&elements=Station_Id_d,lat,lon,alti" + s"&statEles=SUM_PRE" +
      s"&timeRange=[$ymdhm0,$ymdhm1)" +
      "&dataFormat=json&statEleValueRanges=SUM_PRE:[0,999)"
    URLMinRain
  }

  def json2EleX(j: JSONObject): ERain = {
    val sta: Int = j.getIntValue("Station_Id_d")
    val lat = j.getFloat("lat")
    val lon = j.getFloat("lon")
    val alt = ALTChk(j.getString("alti"), lat, lon)
    val sumpre = dt match {
      case  5   => PRE_5mChk(j.getString("SUM_PRE"))
      case 10   => PRE_10mChk(j.getString("SUM_PRE"))
      case _    => PRE_1hChk(lat,lon,j.getString("SUM_PRE"))
    }
    ERain(sta, lat, lon, alt, sumpre)
  }
}
