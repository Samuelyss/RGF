package station.cmiss

import com.alibaba.fastjson.JSONObject
import common.MyConfig._
import org.joda.time.DateTime
import common.TimeTransform._
import station.EleChk._
import scala.collection.concurrent.TrieMap
/**
  * Created by hxf on 16-7-9.
  */
object CmEMinu extends Cmiss[EMinu]{
  def main(args: Array[String]) {
    val ymdhm = "201607080000"
    val map = this (ymdhm)
    println(map.size)
  }

  def apply(ymdhm: String) : TrieMap[Int, EMinu] ={
    val a = getEleXArr("EMinu",ymdhm).toArray
    elex2map(a)
  }
  implicit def toEMinu(x: EMinu): EMinu = x.asInstanceOf[EMinu]

  def mkURLStr(ymdhm_start: String,ymdhm_end: String="") : String= {
    require(ymdhm_start.length == 12)
    val m = getMinutes(ymdhm_start)
    require(m % 10 == 0)
    val ymdhm0 =  ymdhm_start + "00"
    val URLAll = cmiss_http +
      "&interfaceId=getSurfEleByTime&dataCode=SURF_CHN_MAIN_MIN"+
      "&elements=Station_Id_d,Datetime,Lat,Lon,Alti,"+
      "PRS,TEM,RHU,"+
      "GST,GST_5cm,GST_10cm,GST_15cm,GST_20cm,GST_40Cm,LGST"+
      s"&times=$ymdhm0"+ "&dataFormat=json"
    URLAll
  }

  def json2EleX(j : JSONObject) : EMinu = {
    val sta: Int = j.getIntValue("Station_Id_d")
    val lat = j.getFloat("Lat")
    val lon = j.getFloat("Lon")
    val alt = ALTChk(j.getString("alti"),lat,lon)
    val d = j.getDate("Datetime")
    val ymdhm = mk_ymdhm(new DateTime(d))
    val prs = PRSChk(j.getString("PRS"),alt)
    val mon = getMonth(ymdhm)
    val tem = TEMChk(mon,j.getString("TEM"))
    val prsSea = PRS_SeaChk("999999", prs, alt, tem)
    val rhu = RHUChk(j.getString("RHU"))
    val gst = GSTChk(mon,j.getString("GST"))
    val t5cm = GSTChk(mon,j.getString("GST_5cm"))
    val t10cm = GSTChk(mon,j.getString("GST_10cm"))
    val t15cm = GSTChk(mon,j.getString("GST_15cm"))
    val t20cm = GSTChk(mon,j.getString("GST_20cm"))
    val t40cm = GSTChk(mon,j.getString("GST_40Cm"))
    val gtem  = GSTChk(mon,j.getString("LGST"))
    EMinu(ymdhm,sta,lat,lon,alt,prs,tem,rhu,prsSea,gst,t5cm,t10cm,t15cm,t20cm,t40cm,gtem )
  }
}
