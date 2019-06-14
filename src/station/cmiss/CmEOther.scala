package station.cmiss

import com.alibaba.fastjson.JSONObject
import common.MyConfig._
import org.joda.time.DateTime
import common.TimeTransform._
import station.EleChk._
import scala.collection.concurrent.TrieMap
/**
  * Created by 胡骏楠，何险峰，汤沛，北京 on 16-7-9.
  */
object CmEOther extends Cmiss[EOther]{
  def main(args: Array[String]) {
    val ymdhm = "201607080000"
    val map = this (ymdhm)
    println(map.size)
  }

  def apply(ymdhm: String) : TrieMap[Int, EOther] ={
    val a = getEleXArr("EOther",ymdhm).toArray
    elex2map(a)
  }
  implicit def toEOther(x: EOther): EOther = x.asInstanceOf[EOther]

  def mkURLStr(ymdhm_start: String,ymdhm_end: String="") : String= {
    require(ymdhm_start.length == 12)
    val m = getMinutes(ymdhm_start)
    require(m % 10 == 0)
    val ymdhm0 =  ymdhm_start + "00"
    val URLOther = cmiss_http +
      "&interfaceId=getSurfEleByTime&dataCode=SURF_CHN_OTHER_MIN"+
      "&elements=Station_Id_d,Datetime,Lat,Lon,Alti,"+
      "VAP,DPT,PRE_1h,VIS_HOR_1MI,WIN_D_Avg_2mi,WIN_S_Avg_2mi,"+
      s"$ddd_max_nm,$ff_max_nm"+
      s"&times=$ymdhm0"+ "&dataFormat=json"

    URLOther
  }
  def json2EleX(j : JSONObject) : EOther = {
    val sta: Int = j.getIntValue("Station_Id_d")
    val lat = j.getFloat("Lat")
    val lon = j.getFloat("Lon")
    val alt = ALTChk(j.getString("alti"),lat,lon)
    val d = j.getDate("Datetime")
    val ymdhm = mk_ymdhm(new DateTime(d))
    val pre1h = PRE_1hChk(lat,lon,j.getString("PRE_1h"))
    val ffmax = WIN_S_Avg_2miChk(j.getString(ff_max_nm))
    val dddmax = WIN_D_Avg_2miChk(j.getString(ddd_max_nm), ffmax)
    val ff = WIN_S_Avg_2miChk(j.getString("WIN_S_Avg_2mi"))
    val ddd = WIN_D_Avg_2miChk(j.getString("WIN_D_Avg_2mi"), ff)
    val uv = CalcuUV(ddd, ff)
    val u = uv._1
    val v = uv._2
    val vism = VISMChk(j.getString("VIS_HOR_1MI"))
    val vap = j.getFloat("VAP")
    val dpt = j.getFloat("DPT")

    EOther(ymdhm,sta,lat,lon,alt,vap,dpt,pre1h,vism,dddmax,ffmax,ddd,ff,u,v)
  }

}
