package nc

import java.io.File
import java.util

import amtf.Ver
import common.BaseType._
import common.MyConfig
import common.MyConfig._
import common.MyUtils._
import common.TimeTransform._
import grid.GdEleH
import org.joda.time.DateTime
import stat.Advect
import station.{EleHBP, EleHGradient}

/**
  * 产生nc格点场文件。
  * 1. 计算格点场
  * 2. 对格点场输出nc文件
  *
  * Created by 何险峰，北京 on 16-2-22.
  */
object GridNcEleH extends GridNc with EleHGradient{
  //val ncType = "GridNcEleH"
  def main(args: Array[String]) {
    // val args = Array("201607171200","201607121100","201607101200","201607201700","201607201900")//打包注释
    // val args = Array("201609140000","201609141900","201609170000","201609270000")//打包注释
    // val args = Array("201509260600","201510151100")//打包注释
    // val args = Array("201510290000","201510300000")
    //val args = Array("201706010000")
    // val args = Array("201706260000","201707010000")
    //val args = Array("201804161000")//打包注释
    app0(args)
  }

  def app0(args: Array[String]): Unit = {
    val dSrc= MyConfig.dataSrc
    val len = args.length
    len match {
      case 0 => GridNcEleH(DateTime.now.minusHours(8).toString("yyyyMMddHH00"),   dSrc)
      case 1 => GridNcEleH(args(0),   dSrc)
      case 2 => batchApply(args(0), args(1),   dSrc)
      case _ => if (len >= 3) args.foreach(t => GridNcEleH(t,   dSrc))
    }
  }

  def apply(ymdhm: String,
            dSrc: String): Unit = {
    val t0 = System.currentTimeMillis()
      val isFore = false
      val gdEleH = dSrc match {
        case "cmis" => GdEleH(ymdhm, isFore, dSrc)
        case "remote" | "local" | "csvreal" | "csvfit" | "csv0" => GdEleH(ymdhm, isFore, dSrc)
      }
      mkNc4(ymdhm, gdEleH.as3, 0)
      if (nFore > 0) {
        forecast(ymdhm, gdEleH.sta_elesFine, nFore, 1)
      }
      val dt = (System.currentTimeMillis() - t0) / 1000 / 60
      val msg = s"累计花费${dt}分"
      logMsg(ymdhm, "8", "3", "O", s"$ymdhm# $msg")
      logMsg(ymdhm, "8", "3", "I", s"$ymdhm# --------------------------------------------")

  }

  @scala.annotation.tailrec
  def forecast(ymdhm: String, sta_elesInit: StaElesMap, numFore: Int, nf: Int): Unit = {
    val fore_minutes = 10
    val fore_seconds = fore_minutes * 60
    val isFore=true
    if (numFore > 0) {
      val ymdhmFore = ymdhm
      //val ymdhmFore = nextYmdhm_minute(ymdhm,fore_minutes)
      //println(s"预报${ymdhmFore}...")
      val src_egMapArr = sta_af1_2_egMapArr(ymdhm, sta_elesInit)
      val fore = Advect.forecast(src_egMapArr, fore_seconds)

      val adv = Advect(ymdhm, fore, fore, src_egMapArr)
      val nIter = 0
      val sta_elesBP = EleHBP(ymdhmFore, adv, nIter)
      val egMapArrFore = sta_af1_2_egMapArr(ymdhmFore, sta_elesBP)
      val sta_elesFore = arrStaEg2staAf1(egMapArrFore, numEle4Ground)
      val eleHGrid = GdEleH(ymdhmFore, isFore,sta_elesFore)
      mkNc4(ymdhmFore, eleHGrid, nf)
      forecast(ymdhmFore, sta_elesFore, numFore - 1, nf + 1)
    }
  }

  def batchApply(fromYmdhm: String, toYmdhm: String,  dataSrc: String): Unit = {
    println(s"${Ver.ver} $fromYmdhm  -- $toYmdhm ")
    var ymdhm: String = fromYmdhm
    while (ymdhm <= toYmdhm) {
      try {
        isOverWrite match {
          case 0 =>
            val isExisted = new File(getNcPath(ymdhm, 0)).exists()
            if (isExisted)
              println(s"mkNc4($ymdhm) skiped. ")
            else GridNcEleH(ymdhm,  dataSrc)
          case 1 =>
            GridNcEleH(ymdhm,  dataSrc)
          case 2 =>
            val t0 = System.currentTimeMillis()
            GdEleH.apply4sta_csv0(ymdhm,  dataSrc)
            val dt = (System.currentTimeMillis() - t0) / 1000 / 60
            println(s"累计花费${dt}分")
        }
      } catch {
        case e: Exception =>
          val msg = s"批处理失败"
          logMsg(ymdhm, "8", "3", "M", s"$ymdhm# $msg")
      }
      //ymdhm = nextYmdhm_hour(ymdhm)
      ymdhm = if (is10minu) nextYmdhm_minute(ymdhm, 10) else nextYmdhm_hour(ymdhm)
    }
  }

  import ucar.ma2._
  //  import ucar.nc2.constants.{CDM, _Coordinate}
  import ucar.nc2.{Dimension, NetcdfFileWriter, Variable}

  def doMkNc4(ymdhm: String, gd3: AS3, ncFnm: String): Unit = {
    val ncFnmTmp = s"$ncFnm.tmp"
    val areaNm = "china"
    val msg = s"格点场文件$ncFnm"
    logMsg(ymdhm, "8", "3", "O", s"$ymdhm# $msg")

    val ncTmp = mkHead(ymdhm, ncFnmTmp, areaNm, gd3)
    wrtNc(ncTmp,gd3)
    // 由于jNotify发现nc文件，会触发制图, 故用tmp过渡一下
    val tmpFile = new File(ncFnmTmp)
    val ncFile = new File(ncFnm)
    tmpFile.renameTo(ncFile)
  }

  def wrtNc(nc: Nc4Init,gd3:AS3) {
    try {
      nc.writer.create()
      nc.writer.write(nc.time, nc.timeVal)
      nc.writer.write(nc.latitude, nc.latlVal)
      nc.writer.write(nc.longitude, nc.lonlVal)

      for (eleIdx <- 0 until numEle4Ground) {
        eleIdx match {
          case 0 => mkGd3(0, gd3,nc.writer,nc.prs)
          case 1 => mkGd3(1, gd3,nc.writer,nc.prs_sea)
          case 2 => mkGd3(2, gd3,nc.writer,nc.tem)
          case 3 => mkGd3(3, gd3,nc.writer,nc.dpt)
          case 4 => mkGd3(4, gd3,nc.writer,nc.rhu)
          case 5 => mkGd3(5, gd3,nc.writer,nc.vap)
          case 6 => mkGd3(6, gd3,nc.writer,nc.pre_1h)
          case 7 => mkGd3(7, gd3,nc.writer,nc.win_d_avg_2mi)
          case 8 => mkGd3(8, gd3,nc.writer,nc.win_s_avg_2mi)
          case 9 => mkGd3(9, gd3,nc.writer,nc.gst)
          case 10 => mkGd3(10, gd3,nc.writer,nc.gst_5cm)
          case 11 => mkGd3(11, gd3,nc.writer,nc.gst_10cm)
          case 12 => mkGd3(12, gd3,nc.writer,nc.gst_15cm)
          case 13 => mkGd3(13, gd3,nc.writer,nc.gst_20cm)
          case 14 => mkGd3(14, gd3,nc.writer,nc.gst_40cm)
          case 15 => mkGd3(15, gd3,nc.writer,nc.u)
          case 16 => mkGd3(16, gd3,nc.writer,nc.v)
          case 17 => mkGd3(17, gd3,nc.writer,nc.qse)
          case 18 => mkGd3(18, gd3,nc.writer,nc.gtem)
          case 19 => mkGd3(19, gd3,nc.writer,nc.vism)
          case 20 => mkGd3(20, gd3,nc.writer,nc.dddmax)
          case 21 => mkGd3(21, gd3,nc.writer,nc.ffmax)
          case 22 => mkGd3(22, gd3,nc.writer,nc.pre_10m)
          case 23 => mkGd3(23, gd3,nc.writer,nc.pre_5mprev)
          case 24 => mkGd3(24, gd3,nc.writer,nc.pre_5mpost)
          case _ =>
        }
      }
    } finally {
      nc.writer.close()
    }
  }

  case class Nc4Init(writer: NetcdfFileWriter, dims3: util.ArrayList[Dimension],
                     time: Variable, timeVal: ArrayDouble.D1,
                     latitude: Variable, latlVal: ArrayDouble.D1,
                     longitude: Variable, lonlVal: ArrayDouble.D1,
                     prs: Variable,
                     prs_sea: Variable,
                     tem: Variable,
                     dpt: Variable,
                     rhu: Variable,
                     vap: Variable,
                     pre_1h: Variable,
                     win_d_avg_2mi: Variable,
                     win_s_avg_2mi: Variable,
                     gst: Variable,
                     gst_5cm: Variable,
                     gst_10cm: Variable,
                     gst_15cm: Variable,
                     gst_20cm: Variable,
                     gst_40cm: Variable,
                     u: Variable,
                     v: Variable,
                     qse: Variable,
                     gtem: Variable,
                     vism: Variable,
                     dddmax: Variable,
                     ffmax: Variable,
                     pre_10m: Variable,
                     pre_5mprev: Variable,
                     pre_5mpost: Variable
                    )

  def mkHead(ymdhm: String, ncFnm: String, areaNm: String, gd3: AS3): Nc4Init = {
    val head0 = mkHead0(ymdhm, ncFnm, areaNm)
    import head0._
    val prs: Variable = head0.writer.addVariable(root, EleNms.head, DataType.SHORT, dims3)
    addAttr(prs, EleNms.head, "hPa", setFRange(300f, 1100f))

    val prs_sea: Variable = writer.addVariable(root, EleNms(1), DataType.SHORT, dims3)
    addAttr(prs_sea, EleNms(1), "hPa", setFRange(900f, 1100f))

    val tem: Variable = writer.addVariable(root, EleNms(2), DataType.SHORT, dims3)
    addAttr(tem, EleNms(2), "C", setFRange(-50f, 70f))

    val dpt: Variable = writer.addVariable(root, EleNms(3), DataType.SHORT, dims3)
    addAttr(dpt, EleNms(3), "C", setFRange(-50f, 70f))

    val rhu: Variable = writer.addVariable(root, EleNms(4), DataType.SHORT, dims3)
    addAttr(rhu, EleNms(4), "%", setFRange(0f, 100f))

    val vap: Variable = writer.addVariable(root, EleNms(5), DataType.SHORT, dims3)
    addAttr(vap, EleNms(5), "hPa", setFRange(0f, 70f))

    val pre_1h: Variable = writer.addVariable(root, EleNms(6), DataType.SHORT, dims3)
    addAttr(pre_1h, EleNms(6), "mm", setFRange(0f, 200f))

    val win_d_avg_2mi: Variable = writer.addVariable(root, EleNms(7), DataType.SHORT, dims3)
    addAttr(win_d_avg_2mi, EleNms(7), "deg", setFRange(0f, 100f))

    val win_s_avg_2mi: Variable = writer.addVariable(root, EleNms(8), DataType.SHORT, dims3)
    addAttr(win_s_avg_2mi, EleNms(8), "m/s", setFRange(0f, 360f))

    val gst: Variable = writer.addVariable(root, EleNms(9), DataType.SHORT, dims3)
    addAttr(gst, EleNms(9), "C", setFRange(-60f, 90f))

    val gst_5cm: Variable = writer.addVariable(root, EleNms(10), DataType.SHORT, dims3)
    addAttr(gst_5cm, EleNms(10), "C", setFRange(-60f, 90f))

    val gst_10cm: Variable = writer.addVariable(root, EleNms(11), DataType.SHORT, dims3)
    addAttr(gst_10cm, EleNms(11), "C", setFRange(-50f, 70f))

    val gst_15cm: Variable = writer.addVariable(root, EleNms(12), DataType.SHORT, dims3)
    addAttr(gst_15cm, EleNms(12), "C", setFRange(-50f, 70f))

    val gst_20cm: Variable = writer.addVariable(root, EleNms(13), DataType.SHORT, dims3)
    addAttr(gst_20cm, EleNms(13), "C", setFRange(-50f, 70f))

    val gst_40cm: Variable = writer.addVariable(root, EleNms(14), DataType.SHORT, dims3)
    addAttr(gst_40cm, EleNms(14), "C", setFRange(-50f, 70f))

    val u: Variable = writer.addVariable(root, EleNms(15), DataType.SHORT, dims3)
    addAttr(u, EleNms(15), "m/s", setFRange(0f, 100f))

    val v: Variable = writer.addVariable(root, EleNms(16), DataType.SHORT, dims3)
    addAttr(v, EleNms(16), "m/s", setFRange(0f, 100f))

    val qse: Variable = writer.addVariable(root, EleNms(17), DataType.SHORT, dims3)
    addAttr(qse, EleNms(17), "C", setFRange(-50f, 120f))

    val gtem: Variable = writer.addVariable(root, EleNms(18), DataType.SHORT, dims3)
    addAttr(gtem, EleNms(18), "C", setFRange(-60f, 90f))

    val vism: Variable = writer.addVariable(root, EleNms(19), DataType.SHORT, dims3)
    addAttr(vism, EleNms(19), "km", setFRange(0f, 20f))

    val dddmax: Variable = writer.addVariable(root, EleNms(20), DataType.SHORT, dims3)
    addAttr(dddmax, EleNms(20), "deg", setFRange(0f, 360f))

    val ffmax: Variable = writer.addVariable(root, EleNms(21), DataType.SHORT, dims3)
    addAttr(ffmax, EleNms(21), "m/s", setFRange(0f, 100f))

    val pre_10m: Variable = writer.addVariable(root, EleNms(22), DataType.SHORT, dims3)
    addAttr(pre_10m, EleNms(22), "mm", setFRange(0f, 200f))

    val pre_5mprev: Variable = writer.addVariable(root, EleNms(23), DataType.SHORT, dims3)
    addAttr(pre_5mprev, EleNms(23), "mm", setFRange(0f, 200f))

    val pre_5mpost: Variable = writer.addVariable(root, EleNms(24), DataType.SHORT, dims3)
    addAttr(pre_5mpost, EleNms(24), "mm", setFRange(0f, 200f))


    Nc4Init(
      writer, dims3,
      time, timeVal,
      latitude, latlVal,
      longitude, lonlVal,
      prs, prs_sea, tem, dpt, rhu,
      vap, pre_1h,  win_d_avg_2mi, win_s_avg_2mi,  gst,
      gst_5cm, gst_10cm, gst_15cm, gst_20cm, gst_40cm,
      u, v, qse, gtem,  vism,
      dddmax, ffmax,  pre_10m, pre_5mprev, pre_5mpost
    )
  }
}
