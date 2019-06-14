package station.rgwst

import java.io.File
import common.MyUtils._
import stat.Advect

/**
  * Created by 何险峰，北京 on 16-10-13.
  */
object RgwstEleHLocal extends RgwstComm{
  //val rgwstLocDir = "/home/hxf/calf_chk/reanalysis"
  def apply(ymdhm: String): Advect = {
    val msg = s"正在通过文件服务器方式获取 $ymdhm 地面自动站数据...."
    logMsg(ymdhm, "2","1", "I", s"$ymdhm# $msg")
    val t0 = System.currentTimeMillis()
    val v = mkEgArrLocal(ymdhm)
    val dt = (System.currentTimeMillis() - t0) / 1000
    if (v == null) {
      val msg = s"本地文件方式获取地面自动站失败. 花费$dt 秒."
      logMsg(ymdhm, "2","1", "F", s"$ymdhm# $msg")
    } else {
      val msg = s"本地文件方式获取地面自动站ok. 花费$dt 秒."
      logMsg(ymdhm, "2","1", "O", s"$ymdhm# $msg")
    }
    v
  }

  private def mkEgArrLocal(ymdhm: String): Advect= {
    val fnmCur = getRgwstFnm(ymdhm)
    val exists = new File(fnmCur).exists()
    if (exists) mkEgArr(ymdhm) else null
  }

}
