package common
import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by 何险峰，北京 on 15-11-26.
  *     val trainlibsvmDir = s"${libsvmDir}/train${tIdx}"
  * val fitlibsvmDir = s"${libsvmDir}/fit${tIdx}"
  *
  */
object MyConfig {
  case class DataDir(h8_ncDir: String, outNcDir: String)
  val conf: Config = ConfigFactory.parseFile(new File("application.conf"))
  //println(conf.root())
  val comm: Config = conf.getConfig("comm")
  val cmiss_http: String = comm.getString("cmiss_http")
  val hsd_ftpDir: String =  comm.getString("hsd_ftpDir")

  val rgwst_ftp: String = comm.getString("rgwst_ftp")
  val rgwstDir: String = comm.getString("rgwstDir")
  val dataSrc    = comm.getString("dataSrc")
  val hsdDir: String = comm.getString("hsdDir")
  //MyUtils.mkdir(s"$hsdDir/")
  val out: String = comm.getString("outpath")
  val option: Int = comm.getInt("option")
  val csvDir: String = comm.getString("csvDir")
  val mdDir: String = comm.getString("mdDir")
  val nImportanceSamp: Int = comm.getInt("nImportanceSamp")
  val demFnm: String = comm.getString("demFnm")

  val logRMSEDir: String = comm.getString("logRMSEDir")
  val logAbsentDir: String = comm.getString("logAbsentDir")
  val logtype: String = comm.getString("logtype")
  val numMsgIter: Int = comm.getInt("numMsgIter")
  val fitEleNms: Array[String] = comm.getString("fitEleNms").split(",")
  val nFit: Int = fitEleNms.length
  val delayMinutes: Int = comm.getInt("delayMinutes")
  val hsd_delayMinutes: Int = comm.getInt("hsd_delayMinutes")

  val nFore: Int = comm.getInt("nFore")
  val is10minu: Boolean = comm.getBoolean("is10minu")
  val is4chk: Boolean = comm.getBoolean("is4chk")
  val isOverWrite: Int = comm.getInt("isOverWrite")
  val maxEleDz: Double = comm.getDouble("maxEleDz")
  //--------knn val ------------------------------------------
  val knn: Config = conf.getConfig("knn")
  val numNearSta: Int = knn.getInt("numNearSta")
  val numGridNearSta: Int = knn.getInt("numGridNearSta")
  val numGridNearStaExt: Int = 5

  val dict3Fnm: String = knn.getString("dict3Fnm")
  val dictPRSFnm: String = knn.getString("dictPRSFnm")
  val dictRHUFnm: String = knn.getString("dictRHUFnm")
  val dictWindFnm: String = knn.getString("dictWindFnm")
  val dictTEMFnm: String = knn.getString("dictTEMFnm")
  val dict6Fnm: String = knn.getString("dict6Fnm")
  val dictAFnm: String = knn.getString("dictAFnm")
  val dictRFnm: String = knn.getString("dictRFnm")
  val dictVFnm: String = knn.getString("dictVFnm")
  //val nearGFnm       = knn.getString("nearGFnm")
  val dictChkFnm: String = knn.getString("dictChkFnm")
  val dictVISMFnm: String = knn.getString("dictVISMFnm")
  val dictT0Fnm: String = knn.getString("dictT0Fnm")

  val dictGridFnm    : String = knn.getString("dictGridFnm")
  val dictGridExtFnm : String = dictGridFnm + "Ext"

  //-----------------------------------------------------------
  def getDataDir(ymdhm: String): DataDir = {
    // 葵花资料目录
    val h8_ncDir = s"$hsdDir"
    // 地面资料目录
    // val rgwstDir = s"$datapath/bak$year/rgwst$year"
    // 输出nc文件目录 /home/dataUser/himawari/h8_nc/2015/20151108
    val outNcDir = s"$out"
    new File(outNcDir).mkdirs
    //new File(outSatFitNcDir).mkdirs
    DataDir(h8_ncDir, outNcDir)
  }


  def main(args: Array[String]) {
    println(MyConfig.dict3Fnm)
  }
}
