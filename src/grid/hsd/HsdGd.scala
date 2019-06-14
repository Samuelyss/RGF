package grid.hsd

import java.io.File

import common.BaseType._
import common.MyConfig._
import common.MyUtils._
import common.TimeTransform._
import ucar.nc2.dataset.NetcdfDataset

/**
  * 应用张国平使用python产生的nc文件
  * 葵花nc文件中，每个通道的分辨率不同，导致矩阵大小不同。
  * 故每个通道的格点场，有自己的经纬度坐标系统
  * Created by 何险峰 on 16-3-19.
  */

object HsdGd extends HGd{
  def main(args: Array[String]) {
    val hsdGd = this ("201604070130")(0)
    hsdGd.mkPng4test()
  }
  def apply(ymdhm: String): Array[H9GdCase] = {
    val h9GdArr = Array.ofDim[H9GdCase](nBand)
    for (b <- (0 until nBand).par;
         b1 = b + 1; //卫星波段以1为基数
         bxx = if (b1 < 10) s"B0$b1" else s"B$b1";
         ncFnm = getHSDFnm(ymdhm, bxx)) {
      val nc = NetcdfDataset.openDataset(ncFnm)
      val gd: AF2 = var2AF2(nc, bxx)
      val lats = getNcLat(nc)
      val lons = getNcLon(nc)
      nc.close()
      h9GdArr(b) = H9GdCase(lats, lons, gd)
    }
    h9GdArr
  }
  def chkYmdhmFileExists(ymdhm: String): Boolean = {
    val ncFnm = getHSDFnm(ymdhm, "B01")
    val exists = new File(ncFnm).exists()
    if (!exists) {
      val msg = s"${ncFnm}不存在."
      logMsg(ymdhm, "1","1", "M", s"${ymdhm}# $msg")
    }
    exists
  }
  def getHSDFnm(ymdhm: String, bxx: String): String = {
    val year = getYear(ymdhm)
    val ymd = getYmd(ymdhm)
    val hhmm = ymdhm.substring(8)
    require(bxx(0) == 'B', "请以Bxx方式，填入波段！")
    val rxx = bxx match {
      case "B01" | "B02" | "B04" => "R10"
      case "B03" => "R05"
      case _ => "R20"
    }
    val dir = common.MyConfig.getDataDir(ymdhm)
    s"${dir.h8_ncDir}/HS_H08_${ymd}_${hhmm}_${bxx}_FLDK_${rxx}_S0510.nc"
  }
}
