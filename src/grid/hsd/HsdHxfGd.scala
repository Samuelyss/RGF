package grid.hsd

import java.io.File

import common.BaseType._
import common.MyUtils._
import common.TimeTransform._
import ucar.nc2.dataset.NetcdfDataset

/**
  * 读取由hsd产生的格点场
  * Created by 何险峰，北京 on 16-6-28.
  */
object HsdHxfGd extends HGd{
  def main(args: Array[String]) {
    //201606280010
    val h8Gd = this("201609070100")(0)
    h8Gd.mkPng4test()
  }
  def apply(ymdhm: String): Array[H9GdCase] = {
    val t0 = System.currentTimeMillis()
    val h8GdArr = Array.ofDim[H9GdCase](nBand)
    val ncFnm = getHSDFnm(ymdhm)

    val nc = NetcdfDataset.openDataset(ncFnm)
    try {
      val lats = getNcLat(nc)
      val lons = getNcLon(nc)
      for (b <- 0 until nBand;
           b1 = b + 1; //卫星波段以1为基数
           bxx = if (b1 < 10) s"B0$b1" else s"B$b1") {
        val gd: AF2 = var2AF2(nc, bxx)
        h8GdArr(b) = H9GdCase(lats, lons, gd)
      }
    } finally {
      nc.close()
    }
    val dt = (System.currentTimeMillis() - t0)/1000
    val msg = s"卫星格点场输入花费$dt 秒"
    logMsg(ymdhm, "1","1", "O", s"$ymdhm# $msg")
    h8GdArr
  }

  def chkYmdhmFileExists(ymdhm: String): (String,Boolean) = {
    val ncFnm = getHSDFnm(ymdhm)
    val exists = new File(ncFnm).exists()
    (ncFnm,exists)
  }

  def getHSDFnm(ymdhm: String): String = {
    val year = getYear(ymdhm)
    val ymd = getYmd(ymdhm)
    val hhmm = ymdhm.substring(8)
    val dir = common.MyConfig.getDataDir(ymdhm)
    val fnm = s"${dir.h8_ncDir}/$year/$ymd/HS_H08_${ymd}_${hhmm}_B01_FLDK_R10_S0110.nc"
    fnm
  }
}
