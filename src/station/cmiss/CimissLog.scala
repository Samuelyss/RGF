package station.cmiss

import java.io.File

import common.MyConfig.logRMSEDir
import common.MyUtils._

object CimissLog {

  def logCimissWork(ymdhm: String,workMsg : String): Unit ={
    val tp = "cimiss到站统计文件"
    println(tp)
    println("时间，要素名，缺测，到站率，实到，应到")
    println(workMsg)
    mkFile(ymdhm,logRMSEDir.replaceAll("RMSE", "cimiss"), workMsg,tp)
  }
  def cimissAbent(ymdhm: String):Boolean={
    val rdir = logRMSEDir.replaceAll("RMSE", "cimiss")
    val fnm = mkTxtFilePath(ymdhm,rdir)
    val f = new File(fnm)
    val e = f.exists()
    val s = if (e) f.length() else 0
    s < 2000
  }
}
