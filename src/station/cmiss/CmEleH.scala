package station.cmiss

import com.alibaba.fastjson.JSONObject
import common.MyConfig._
import org.joda.time.DateTime
import common.TimeTransform._
import station.EleChk._

import scala.collection.concurrent.TrieMap
import dict.StaDict
import station.EleHOp

/**
  * Created by 胡骏楠，何险峰，汤沛， 北京 on 16-7-9.
  */
object CmEleH extends Cmiss[EleH] with EleHOp{
  def main(args: Array[String]) {
    val ymdhm = "201607080000"
    val map = this (ymdhm)
    println(map.size)
  }

  def apply(ymdhm: String,isBind2dict : Boolean=true) : TrieMap[Int, EleH] ={
    val a = getEleXArr("EleH",ymdhm).toArray
    elex2map(a,isBind2dict)
  }

  implicit def toEleH(x: EleH): EleH = x.asInstanceOf[EleH]

  def elex2map(a : Array[EleH],isBind2dict : Boolean): TrieMap[Int, EleH]={
    val trie = TrieMap[Int, EleH]()
    if (a.nonEmpty){
      a.par.foreach{f =>
        if (isBind2dict && StaDict.dictStaAll.keySet(f.sta) )
        trie +=((f.sta,f))
      }
    }
    trie
  }

  def mkURLStr(ymdhm_start: String,ymdhm_end: String="") : String={
    require(ymdhm_start.length == 12)
    val m = getMinutes(ymdhm_start)
    require(m == 0,"ymdhm must be ymdh00 format.")
    val ymdhm0 = ymdhm_start + "00"
    val URLStr =
    // "http://10.20.76.31:8008/cimiss-web/api?userId=HX_RY&pwd=renying123"+
      cmiss_http +
        "&interfaceId=getSurfEleByTime&dataCode=SURF_CHN_MUL_HOR"+
        "&elements=Station_Id_d,Datetime,Lat,Lon,Alti,"+
        "PRS,PRS_Sea,TEM,DPT,RHU,VAP,PRE_1h,WIN_D_Avg_2mi,WIN_S_Avg_2mi,"+
        "GST,GST_5cm,GST_10cm,GST_15cm,GST_20cm,GST_40Cm,LGST,VIS_HOR_1MI,"+
        s"$ddd_max_nm,$ff_max_nm"+
        s"&times=$ymdhm0"+
        "&dataFormat=json"
    URLStr
  }
  def json2EleX(j : JSONObject): EleH = {
    val sta: Int = j.getIntValue("Station_Id_d")
    val lat = j.getFloat("Lat")
    val lon = j.getFloat("Lon")
    val alt = ALTChk(j.getString("alti"),lat,lon)
    val d = j.getDate("Datetime")
    val ymdhm = mk_ymdhm(new DateTime(d))
    val mon = getMonth(ymdhm)
    val pre1h = PRE_1hChk(lat,lon,j.getString("PRE_1h"))
    val prs = PRSChk(j.getString("PRS"),alt)
    val tem = TEMChk(mon,j.getString("TEM"))
    val prsSea = PRS_SeaChk(j.getString("PRS_Sea"), prs, alt, tem)
    val rhu = RHUChk(j.getString("RHU"))

    val ff = WIN_S_Avg_2miChk(j.getString("WIN_S_Avg_2mi"))
    val ddd = WIN_D_Avg_2miChk(j.getString("WIN_D_Avg_2mi"), ff)
    val uv = CalcuUV(ddd, ff)
    val u = uv._1
    val v = uv._2
    val ffmax0 = WIN_S_Avg_2miChk(j.getString(ff_max_nm))
    val ffmax = if (ffmax0.isNaN) ff else ffmax0

    val dddmax0 = WIN_D_Avg_2miChk(j.getString(ddd_max_nm), ffmax)
    val dddmax = if (dddmax0.isNaN) ddd else dddmax0

    val vism = VISMChk(j.getString("VIS_HOR_1MI"))
    val vap = VAPChk(j.getString("VAP"), tem, rhu)
    val dpt = DPTChk(mon,j.getString("DPT"), tem, vap)
    val qse = calcuQse(prs, tem, dpt, vap)
    val gst = GSTChk(mon,j.getString("GST"))
    val t5cm = GSTChk(mon,j.getString("GST_5cm"))
    val t10cm = GSTChk(mon,j.getString("GST_10cm"))
    val t15cm = GSTChk(mon,j.getString("GST_15cm"))
    val t20cm = GSTChk(mon,j.getString("GST_20cm"))
    val t40cm = GSTChk(mon,j.getString("GST_40Cm"))
    val gtem  = GSTChk(mon,j.getString("LGST"))
    val pre10m = Float.NaN
    val pre5mprev = Float.NaN
    val pre5mpost = Float.NaN

    val numNaN = Array(prs, prsSea, tem, dpt, rhu, vap, pre1h, ddd, ff,
      gst, t5cm, t10cm, t15cm, t20cm, t40cm, u, v, qse, gtem, vism, dddmax, ffmax, pre10m)
      .count(f => f.isNaN)
    val eleh = if (numNaN == numEle4Ground) null  else EleH(
      ymdhm, sta, lat, lon, alt, prs, prsSea, tem, dpt, rhu, vap, pre1h, ddd, ff,
      gst, t5cm, t10cm, t15cm, t20cm, t40cm, u, v, qse, gtem, vism, dddmax, ffmax,
      pre10m,pre5mprev,pre5mpost)
    eleh
  }
}
