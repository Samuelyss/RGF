package hsd.amtf
import java.io.{File, PrintWriter}
import common.MyConfig
import hsd.hisd.HsdConfig
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import common.TimeTransform._
/**
 * Created by 何险峰，北京 on 15-11-26 修改.
 */
object Utils {
  def i2ii(i:Int):String={
    //if (i < 10) s"0${i}" else s"${i}"
    "%02d".format(i)
  }

  def getBandNo(fnm: String):Int={
    val pattern1="_B[0-9]+_".r
    val bandStrP="[0-9]+".r
    val nm = new File(fnm).getName
    val s1= pattern1.findFirstIn(nm).get
    bandStrP.findFirstIn(s1).get.toInt
  }
  def prevYmdhm_minute(timeStr : String, nMinute : Int):String={
    val ymdhm_fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val datetime = DateTime.parse(timeStr, ymdhm_fmt)
    val dtPrev = datetime.minusMinutes(nMinute)
    val ymdhmPrevStr = ymdhm_fmt.print(dtPrev)
    ymdhmPrevStr
  }

  def getTimeStr(fnm: String):String={
    val pattern1="_[0-9]+_[0-9]+_".r
    val timeStrP="[0-9]+_[0-9]+".r
    val nm = new File(fnm).getName
    val s1= pattern1.findFirstIn(nm).get
    timeStrP.findFirstIn(s1).get
  }
  def lstFile(root: String, ext: String): IndexedSeq[scala.reflect.io.Path] = {
    scala.reflect.io.Path(root).walkFilter(p => p.isDirectory || p.path.endsWith(ext)).toIndexedSeq
  }
  def save2csv(csvFnm : String,arr2d: Array[Array[Short]]){
    val wrt = new PrintWriter(new File(s"${csvFnm}"))
    arr2d.foreach{f=>
      wrt.println(f.mkString(","))
    }
    wrt.close
  }
  def mkdir(path: String) {
    val f = new File(path)
    if (path(path.length-1) == '/') {
      f.mkdirs()
    } else if (!f.getParentFile.exists) {
      f.getParentFile().mkdirs
    }
  }

  def clearRamDisk(ramdisk:String): Unit ={
    val fRamdisk = new File(ramdisk)
    fRamdisk.mkdir()
    val files = fRamdisk.listFiles
    files.foreach(f => f.delete)
  }

  def getHSDFnm(ymdhm: String): String = {
    val year = getYear(ymdhm)
    val ymd = getYmd(ymdhm)
    val hhmm = ymdhm.substring(8)
    s"${MyConfig.hsdDir}/$year/$ymd/HS_H08_${ymd}_${hhmm}_B01_FLDK_R10_S0110.nc"
  }

  def chkYmdhmFileExists(ymdhm: String): Boolean = {
    val ncFnm = getHSDFnm(ymdhm)
    val exists = new File(ncFnm).exists()
    if (!exists) println(s"创建 $ncFnm ...")
    exists
  }
}
