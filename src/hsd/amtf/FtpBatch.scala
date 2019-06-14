package hsd.amtf

import common.MyConfig._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}
import hsd.amtf.Utils._

object FtpBatch {
  val ramdisk:String="/tmp/ram"
  def main(args: Array[String]): Unit = {
    //val args = Array("20151011","20171012")
    println(s"the output dir is : $hsdDir")
    val len = args.length
    if (len <=0 || len > 2) {
      println("please input as :")
      println("java -classpath hsd1.jar amtf.FtpBatch 20161001 20161031")
    } else if (len == 1){
      val ymd = args(0)
      doLocal(ymd,hsdDir)
    } else {
      val ymd0 = args(0)
      val ymd1 = args(1)
      genDaysNc(ymd0,ymd1)
    }
  }

  //对ymd0 ---- ymd1 的ftp葵花数据合成nc文件集到ncDir
  private def genDaysNc(ymd0:String,ymd1:String): Unit ={
    def nextYmd(ymd : String):String={
      val ymd_fmt = DateTimeFormat.forPattern("yyyyMMdd")
      val datetime = DateTime.parse(ymd, ymd_fmt)
      val nextDay = datetime.plusDays(1)
      val nextDayStr = ymd_fmt.print(nextDay)
      nextDayStr
    }
    var d = ymd0
    while (d <= ymd1){
      doLocal(d,hsdDir)
      d = nextYmd(d)
    }
  }
  //完成一天24时次的nc合成
  private def doLocal(ymd: String,ncDir:String): Unit ={
    //val ncDir4ymd = s"$ncDir"
    for (hour <- 0 to 23){
      val hh = i2ii(hour)
      for (m <- 0 to 5 ) {
        val mm = i2ii(m*10)
        val hhmm = s"$hh$mm"
        val ymdhm = s"$ymd$hhmm"
        println(s"$ymd $hhmm on doing ......")
        val t = Try {
          clearRamDisk(ramdisk)
          val ncExists = Utils.chkYmdhmFileExists(ymdhm)
          if (!ncExists) BandNc(hsd_ftpDir,ymd,hhmm,ncDir,needRmDatFnm=true,ramdisk)
        }
        t match {
          case Success(v) =>
            println(s"$ymd 卫星处理成功。 " + v)
          case Failure(e) =>
            println(s"$ymd 卫星处理失败。"+ e.getMessage)
        }
      }
    }
  }
}
