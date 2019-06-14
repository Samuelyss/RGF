package hsd.amtf

import java.io.File

import Utils._
import hsd.hisd.{Phy, Profile}
import hsd.hisd.HsdConfig._
import hsd.nc.HimaNc
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/**
  * Created by 何险峰，成都 on 2015/10/4.
  */

object BandNc {
  import hsd.hisd.DataType._
  import sys.process._
  def main(args: Array[String]): Unit = {
    test1
  }

  def test1: Unit = {
    val ymd = "20160930"
    val hhmm = "0600"
    val ncDir = "/wk2/hsd/h8_nc"
    //val needRmDatFnm: Boolean = true
    val hsd_ftpDir = "ftp://pub_data:xxshj@10.1.72.41//SATE/Himawari-8/fulldisk/HSD"
    this (hsd_ftpDir,ymd, hhmm, ncDir, true,"/tmp/ramdisk")
  }

  def test0: Unit = {
    val swathFnms = Array(
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0110.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0210.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0310.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0410.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0510.DAT"
    )
    val ymd = "20150914"
    val year = ymd.substring(0, 4)
    val ncDir = "/wk2/hsd/h8_nc"
    val ncDir4ymd = s"${ncDir}/${year}/${ymd}"
    this (swathFnms, ncDir, ncDir4ymd)
  }

  def apply(hsd_ftpDir:String,ymdhm: String, ncDir: String,ramdisk:String): Unit = {
    //val ymdhm_10 = prevYmdhm_minute(ymdhm,10)
    val ymd = ymdhm.substring(0, 8)
    val hhmm = ymdhm.substring(8)
    val t = Try {
      clearRamDisk(ramdisk)
      val ncExists = Utils.chkYmdhmFileExists(ymdhm)
      if (!ncExists) this (hsd_ftpDir,ymd, hhmm, ncDir, true,ramdisk)
    }
    t match {
      case Success(v) =>
        println(s"$ymdhm 卫星处理成功。 " + v)
      case Failure(e) =>
        println(s"$ymdhm 卫星处理失败。"+ e.getMessage)
    }
  }


  def apply(hsd_ftpDir:String,ymd: String, hhmm: String, ncDir: String, needRmDatFnm: Boolean,ramdisk:String): Unit = {
    //cleanTmp(ymd,hhmm)
    val t0 = System.currentTimeMillis()
    val phys = Array.ofDim[Phy](nBand)
    for (i <- (0 until nBand).par) {
      val band = i + 1
      val profile = Profile(hsd_ftpDir,ymd, hhmm, band, needRmDatFnm,ramdisk)
      phys(i) = Phy(profile)
    }
    val year = ymd.substring(0, 4)
    val ncDir4ymd = s"$ncDir/${year}/${ymd}"
    new File(ncDir4ymd).mkdirs
    val t = Try {
      mkBandNc(phys, ncDir4ymd)
    }
    t match {
      case Success(v) =>
        println(s"$ymd 卫星处理成功。 " + v)
      case Failure(e) =>
        println(s"$ymd 卫星处理失败。"+ e.getMessage)
    }
    val t1 = System.currentTimeMillis()
    val dt = (t1 - t0)/1000
    println(s"建立$ymd$hhmm 卫星nc文件，花费$dt s ")
  }

  def apply(swathFnms: AStr1, ncDir: String, ncDir4ymd: String): Unit = {
    val profile = Profile(swathFnms)
    val phy = Phy(profile)
    mkBandNc(phy, ncDir, ncDir4ymd)
  }
  def ymd_hm2ym_d_hm(ymd_hmStr: String):String={
    val y_m_d_hms_fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:00")
    val datetime = DateTime.parse(ymd_hmStr, DateTimeFormat.forPattern("yyyyMMdd_HHmm"))
    val y_m_d_hmStr = y_m_d_hms_fmt.print(datetime)
    // println(s"===========${y_m_d_hmStr}=============")
    y_m_d_hmStr
  }
  def ymd_hm2ymdhm(ymd_hmStr: String):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(ymd_hmStr, DateTimeFormat.forPattern("yyyyMMdd_HHmm"))
    val ymdhmStr = ymdhm_fmt.print(datetime)
    // println(s"===========${y_m_d_hmStr}=============")
    ymdhmStr
  }

  def mkBandNc(phy: Phy, ncDir: String, ncDir4ymd: String): Unit = {
    val f = new File(phy.fnm)
    val ncFnm = f.getName.replace(".DAT", ".nc")
    val ymd_hm = getTimeStr(ncFnm)

//    println(s"timeStr: ${ymd_hm}")
    val ofnm = s"$ncDir/$ncFnm"
    val ymdhm= ymd_hm2ymdhm(ymd_hm)
    HimaNc(ofnm, ymdhm, Array(phy))
  }
  def mkBandNc(phys: Array[Phy], ncDir4ymd: String): Unit = {
    val f = new File(phys(0).fnm)
    val ncFnm: String = f.getName.replace(".DAT", ".nc")
    //val ncFnmTmp = s"$ncFnm.tmp"
    val ymd_hm = getTimeStr(ncFnm)
    val ofnm = s"$ncDir4ymd/$ncFnm"
    val ofnmTmp = s"$ofnm.tmp"
    //println(s"[BandNc.mkBandNc in] timeStr: ${timeStr} $ncFnm")
    val ymdhm= ymd_hm2ymdhm(ymd_hm)
    HimaNc(ofnmTmp, ymdhm, phys)

    val tmpFile = new File(ofnmTmp)
    val ncFile = new File(ofnm)
    tmpFile.renameTo(ncFile)
    //println(s"[BandNc.mkBandNc out] timeStr: ${timeStr} $ncFnm")
  }

}
