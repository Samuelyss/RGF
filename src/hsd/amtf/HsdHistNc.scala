package hsd.amtf

import java.io.File

import Utils._
import hsd.hisd.{Phy, Profile}
import hsd.nc.HimaNc
import hsd.hisd.HsdConfig._

/**
  * Created by 何险峰，北京 on 16-7-9.
  */
object HsdHistNc {

  //完成一天24时次的nc合成
  def doLocal(ymd: String,ncDir:String): Unit ={
    val ncDir4ymd = s"$ncDir"
    for (hour <- 0 to 23){
      val hh = i2ii(hour)
      for (m <- 0 to 5 ) {
        val mm = i2ii(m*10)
        val hhmm = s"$hh$mm"
        val phys = Array.ofDim[Phy](nBand)
        //val srcDir4ymd = s"${srcDir}/${ymd}"
        for (band <- 1 to nBand) {
          val swathFnms = getBandFnms(ymd, hhmm, band - 1,ncDir)
          val profile = Profile(swathFnms)
          phys(band - 1) = Phy(profile)
        }
        //println(s"[mkBandNc in]${ymd},${hhmm}")
        mkBandNc(phys, ncDir4ymd)
        //println(s"[mkBandNc out]${ymd},${hhmm}")
      }
    }
  }
  def getBandFnms(ymd:String, hhmm:String, band_i : Int,ncDir:String) ={
    val _B_R =s"_B${bands(band_i).n}_FLDK_R${bands(band_i).res}"
    val _S = Range(1,numSwath).inclusive.map(swath => if (swath<10) s"_S0${swath}10" else s"_S${swath}10")
    val swathFnms = Array.fill[String](_S.length)("")
    //val i = band_i -1
    for (i <- (0 until numSwath).par ){
      val swathFnm0 = s"HS_H08_${ymd}_$hhmm${_B_R}${_S(i)}.DAT"
      val swathFnm = s"$ncDir/$ymd/$swathFnm0"
      swathFnms(i) = if (new File(swathFnm).exists()) swathFnm else ""
    }
    swathFnms.filter(f => f.length>1)
  }
  def mkBandNc(phys: Array[Phy],ncDir4ymd : String): Unit = {
    val f = new File(phys(0).fnm)
    val ncFnm = f.getName.replace(".DAT", ".nc")
    val timeStr = getTimeStr(ncFnm)
    val ofnm = s"$ncDir4ymd/$ncFnm"
    HimaNc(ofnm, timeStr, phys)
  }
}
