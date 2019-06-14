package station.rgwst

import java.io.File
import common.MyConfig._
import common.MyUtils._
import common.TimeTransform._
import stat.Advect

/**
  * Created by 何险峰，北京 on 16-6-27.
  */
object RgwstEleHRemote extends RgwstComm{
  def main(args: Array[String]): Unit = {
    val ymdhm = "201610150810"
    val adv = this (ymdhm)
    println(adv.mend.length)
  }

  def apply(ymdhm: String): Advect = {
    val msg = s"正在通过远程文件服务器方式获取地面自动站数据...."
    logMsg(ymdhm, "2","1", "I", s"$ymdhm# $msg")
    val t0 = System.currentTimeMillis()
    val v = wget_MkEgArr(ymdhm)
    val dt = (System.currentTimeMillis() - t0) / 1000
    if (v == null) {
      val msg = s"远程文件服务器方式获取地面自动站失败. 花费${dt}秒."
      logMsg(ymdhm, "2","1", "F", s"$ymdhm# $msg")
    } else {
      val msg = s"远程文件服务器方式获取地面自动站成功. 花费${dt}秒."
      logMsg(ymdhm, "2","1", "O", s"$ymdhm# $msg")
    }
    v
  }
  private def wget_MkEgArr(ymdhm: String): Advect= {
    val fnmCur = getRgwstFnm(ymdhm)
    val exists0 = new File(fnmCur).exists()
    val wgetResult0 = if (!exists0) wget_rgwst(ymdhm) else 0

    val minu = getMinutes(ymdhm)
    val prevYmdh0 = if (minu == 0) mkYmdh0(prevYmdhm_hour(ymdhm,1)) else mkYmdh0(ymdhm)
    val fnmPrev = getRgwstFnm(prevYmdh0)
    val exists1 = new File(fnmPrev).exists()
    val wgetResult1 = if (!exists1) wget_rgwst(prevYmdh0) else 0

    if (wgetResult0 == 0 && wgetResult1 == 0) mkEgArr(ymdhm) else null
  }

  private def wget_rgwst(ymdhm: String):Int = {
    import sys.process._
    val rgwstFnm = s"rgwst_${ymdhm}_005.txt"

    val ftp = s"$rgwst_ftp/$rgwstFnm"
    val wgetResult = s"wget -c -q -P $rgwstDir $ftp" !

    Thread.sleep(10)
    wgetResult
  }
}
