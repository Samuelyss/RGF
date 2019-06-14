package hsd.hisd

import java.io.File

import breeze.io.{ByteConverter, ByteConverterBigEndian, ByteConverterLittleEndian, RandomAccessFile}

import scala.collection.mutable.ListBuffer

/**
 * Created by 何险峰,张国平，高金兵on 15-9-23.
 */
case class Head
(fnm : String,basic: Basic_Info, data: Data_Info, proj: Proj_Info, nav: Navi_Info,
 calib: Calib_Info, interCalib: InterCalib_Info, seg: Segm_Info,
 navcorr: NaviCorr_Info, obstime: ObsTime_Info, error: Error_Info, spare: Spare,
 cdata : Array[Array[Short]])
object Head {
  def main(args: Array[String]) {
    val fnm = "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0110.DAT"
     Head(fnm)
  }
  def apply(fnm: String):Head = {
    val raf = new RandomAccessFile(fnm, "r")(ByteConverterLittleEndian)
    //val raf = new RandomAccessFile(fnm, "r")(getByteConverter(fnm))
    try {
      val bandNo = getBandNo(fnm)
      val basic = Basic_Info(raf) /* 1 */
      val data = Data_Info(raf) /* 2 */
      val proj = Proj_Info(raf) /* 3 */
      val nav = Navi_Info(raf) /* 4 */
      val calib: Calib_Info = if (bandNo >= 7) Calib_Info_a(raf) else Calib_Info_b(raf) /* 5 */
      val interCalib : InterCalib_Info= basic.verName match {
          case "1.0" => InterCalib_Info_10(raf) /* 6 */
          case "1.1" => InterCalib_Info_11(raf) /* 6 */
          case "1.2"|"1.3" => InterCalib_Info_12(raf) /* 6 */
        }
      val seg = Segm_Info(raf) /* 7 */
      val navcorr = NaviCorr_Info(raf) /* 8 */
      val obstime = ObsTime_Info(raf) /* 9 */
      val error = Error_Info(raf) /*10 */
      val spare = Spare(raf) /*11 */
      val t1 = List(basic.BlockLen, data.BlockLen, proj.BlockLen, nav.BlockLen,
          calib.BlockLen, interCalib.BlockLen, seg.BlockLen, navcorr.BlockLen,
          obstime.BlockLen, error.BlockLen, spare.BlockLen).sum
      val t2 = basic.totalHeaderLen
      require(t1 == t2, s"Total header length of$t2, not equal to sum of first 11 headers of $t1")
      val cdata = CData(raf, data.nLin, data.nPix)   /*11 */
      //println(s"nLin:${data.nLin},nPix:${data.nPix}")
      Head(fnm, basic, data, proj, nav,calib, interCalib, seg, navcorr, obstime, error, spare, cdata)
    } finally raf.close
  }
  private def getBandNo(fnm: String):Int={
    val pattern1="_B[0-9]+_".r
    val bandStrP="[0-9]+".r
    val nm = new File(fnm).getName
    val s1= pattern1.findFirstIn(nm).get
    bandStrP.findFirstIn(s1).get.toInt
  }
  private def getByteConverter(fnm : String):ByteConverter={
    val raf = new RandomAccessFile(fnm, "r")(ByteConverterLittleEndian)
    try {
      val basic = Basic_Info(raf)
      if (basic.BlockLen == 282 || basic.byteOrder==0)
        ByteConverterLittleEndian
      else
        ByteConverterBigEndian
    } finally raf.close
  }
}

trait Hisd {
    def u1(raf: RandomAccessFile): Int = raf.readUInt8
    def u2(raf: RandomAccessFile): Int = raf.readUInt16
    def u4(raf: RandomAccessFile): Long = raf.readUInt32
    def i4(raf: RandomAccessFile): Int = raf.readInt32
    def s(raf: RandomAccessFile, n: Int): String = new String(Array.fill[Char](n)(raf.readByte.toChar)).trim
    def f8(raf: RandomAccessFile): Double = raf.readDouble
    def f4(raf: RandomAccessFile): Float = raf.readFloat
    def c(raf: RandomAccessFile):Char = raf.readByte().toChar
    def i2(raf: RandomAccessFile, nLin:Int, nPix:Int):Array[Array[Short]] = {
      val vect = raf.readInt16(nLin * nPix)
      vect.grouped(nPix).toArray
    }
}

/* #1 */
case class Basic_Info
(HeaderNum: Int, BlockLen: Int, headerNum: Int, byteOrder: Int,
 satName: String, proName: String, ObsType1: String, ObsType2: String,
 TimeLine: Int, ObsStartTime: Double, ObsEndTime: Double, fileCreationMjd: Double,
 totalHeaderLen: Long, dataLen: Long, qflag1: Int, qflag2: Int, /*16 */
 qflag3: Int, qflag4: Int, verName: String, fileName: String, spare: String)

object Basic_Info extends Hisd {
  def apply(raf: RandomAccessFile): Basic_Info = {
    Basic_Info(u1(raf), u2(raf), u2(raf), u1(raf),
      s(raf, 16), s(raf, 16), s(raf, 4), s(raf, 2),
      u2(raf), f8(raf), f8(raf), f8(raf),
      u4(raf), u4(raf), u1(raf), u1(raf),
      u1(raf), u1(raf), s(raf, 32), s(raf, 128), s(raf, 40))
  }
}

/* #2 */
case class Data_Info
( HeaderNum: Int,  BlockLen: Int, bitPix: Int, nPix: Int,
 nLin: Int, comp: Int, spare: String)

object Data_Info extends Hisd {
  def apply(raf: RandomAccessFile): Data_Info = {
    Data_Info(u1(raf), u2(raf), u2(raf), u2(raf), u2(raf), u1(raf), s(raf, 40))
  }
}

/* #3 */
case class Proj_Info
(HeaderNum: Int,  BlockLen: Int,  subLon: Double, cfac: Long,
 lfac: Long, coff: Float,  loff: Float, satDis: Double,
 eqtrRadius: Double, polrRadius: Double, projParam1: Double, projParam2: Double,
 projParam3: Double, projParamSd: Double, resampleKind: Int, resampleSize: Int,
 spare: String)

object Proj_Info extends Hisd {
  def apply(raf: RandomAccessFile): Proj_Info = {
    Proj_Info(u1(raf), u2(raf), f8(raf), u4(raf),
      u4(raf), f4(raf), f4(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), u2(raf), u2(raf),
      s(raf, 40))
  }
}

/* #4 */
case class Navi_Info
(HeaderNum: Int,  BlockLen: Int,  navMjd: Double,  sspLon: Double,
  sspLat: Double,  satDis: Double,  nadirLon: Double,  nadirLat: Double, /* 8 */
  sunPos_x: Double,  sunPos_y: Double,  sunPos_z: Double,  moonPos_x: Double, /*12 */
  moonPos_y: Double,  moonPos_z: Double,  spare: String)

object Navi_Info extends Hisd {
  def apply(raf: RandomAccessFile): Navi_Info = {
    Navi_Info(u1(raf), u2(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), s(raf, 40))
  }
}

/* #5 */
/* for band 7-16 */
trait Calib_Info{
  val HeaderNum: Int
  val BlockLen: Int
  val bandNo: Int
  val waveLen: Double
  val bitPix: Int
  val errorCount: Int
  val outCount: Int
  val gain_cnt2rad: Double
  val cnst_cnt2rad: Double
  //---------------------------------------------------------
  val rad2btp_c0: Double
  val rad2btp_c1: Double
  val rad2btp_c2: Double
  val btp2rad_c0: Double
  val btp2rad_c1: Double
  val btp2rad_c2: Double
  val lightSpeed: Double
  val planckConst: Double
  val bolzConst: Double
  val spare: String
  //----------------------------------------------------------
  val rad2albedo: Double
  val spareV: String
  def isCalib_Info_a : Boolean
}
case class Calib_Info_a
( HeaderNum: Int,  BlockLen: Int,  bandNo: Int,  waveLen: Double, /* 4 */
  bitPix: Int,  errorCount: Int,  outCount: Int,  gain_cnt2rad: Double, /* 8 */
  cnst_cnt2rad: Double,
  rad2btp_c0: Double,  rad2btp_c1: Double,  rad2btp_c2: Double, /*12 */
  btp2rad_c0: Double,  btp2rad_c1: Double,  btp2rad_c2: Double,  lightSpeed: Double, /*16 */
  planckConst: Double,  bolzConst: Double,  spare: String) extends Calib_Info{
  val rad2albedo: Double=Double.NaN
  val spareV=""
  def isCalib_Info_a : Boolean = true
}

object Calib_Info_a extends Hisd {
  def apply(raf: RandomAccessFile): Calib_Info_a = {
    Calib_Info_a(u1(raf), u2(raf), u2(raf), f8(raf),
      u2(raf), u2(raf), u2(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), s(raf, 40))
  }
}
/* for band 1-6 */
case class Calib_Info_b
( HeaderNum: Int,  BlockLen: Int,  bandNo: Int,  waveLen: Double, /* 4 */
  bitPix: Int,  errorCount: Int,  outCount: Int,  gain_cnt2rad: Double, /* 8 */
  cnst_cnt2rad: Double,
  rad2albedo: Double,  spareV: String) extends Calib_Info{
  val rad2btp_c0: Double =Double.NaN
  val rad2btp_c1: Double =Double.NaN
  val rad2btp_c2: Double =Double.NaN
  val btp2rad_c0: Double =Double.NaN
  val btp2rad_c1: Double =Double.NaN
  val btp2rad_c2: Double =Double.NaN
  val lightSpeed: Double =Double.NaN
  val planckConst: Double =Double.NaN
  val bolzConst: Double =Double.NaN
  val spare= ""
  def isCalib_Info_a : Boolean = false
}

object Calib_Info_b extends Hisd {
  def apply(raf: RandomAccessFile): Calib_Info_b = {
    Calib_Info_b(u1(raf), u2(raf), u2(raf), f8(raf),
      u2(raf), u2(raf), u2(raf), f8(raf),
      f8(raf), f8(raf), s(raf, 104))
  }
}

/* #6 */
trait InterCalib_Info {          /* 1.0,1.1,1.2*/
  val HeaderNum: Int             /* 1 , 1 , 1  */
  val BlockLen: Int              /* 2 , 2 , 2  */
  val gsicsCorr_C: Double        /* 3 , 3 , 3  */
  val gsicsCorr_C_er: Double     /* 4 , 4 , -  */
  val gsicsCorr_1: Double        /* 5 , 5 , 4  */
  val gsicsCorr_1_er: Double     /* 6 , 6 , -  */
  val gsicsCorr_2: Double        /* 7 , 7 , 5  */
  val gsicsCorr_2_er: Double     /* 8 , 8 , -  */
  val gsicsBias : Double         /* - , - , 6  */
  val gsicsUncert : Double       /* - , - , 7  */
  val gsicsStscene: Double       /* - , - , 8  */
  val gsicsCorr_StrMJD: Double   /* 9 , 9 , 9  */
  val gsicsCorr_EndMJD: Double   /*10 ,10 , 10 */
  val gsicsCorrInfo: String      /*11 , - , -   64  */
  val gsicsUpperLimit: Float     /* - ,11 , 11 */
  val gsicsLowerLimit: Float     /* - ,12 , 12 */
  val gsicsFilename: String      /* - ,13 , 13   128*/
  val spare: String              /*12 ,14 , 14   128*/
}
case class InterCalib_Info_10
( HeaderNum: Int,  BlockLen: Int,  gsicsCorr_C: Double,  gsicsCorr_C_er: Double, /* 4 , 4 , -  */
  gsicsCorr_1: Double,  gsicsCorr_1_er: Double,  gsicsCorr_2: Double,  gsicsCorr_2_er: Double, /* 8 , 8 , -  */
  gsicsCorr_StrMJD: Double,  gsicsCorr_EndMJD: Double,  gsicsCorrInfo: String,  spare: String) extends InterCalib_Info{
  val gsicsBias : Double= Double.NaN
  val gsicsUncert : Double= Double.NaN
  val gsicsStscene : Double= Double.NaN
  val gsicsUpperLimit: Float = Float.NaN
  val gsicsLowerLimit: Float = Float.NaN
  val gsicsFilename = ""
}
object InterCalib_Info_10 extends Hisd {
  def apply(raf: RandomAccessFile): InterCalib_Info_10 = {
    InterCalib_Info_10(u1(raf), u2(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), s(raf, 64), s(raf, 128))
  }
}

case class InterCalib_Info_11
( HeaderNum: Int,  BlockLen: Int,  gsicsCorr_C: Double,  gsicsCorr_C_er: Double,
  gsicsCorr_1: Double,  gsicsCorr_1_er: Double,  gsicsCorr_2: Double,  gsicsCorr_2_er: Double, /* 8 , 8 , -  */
  gsicsCorr_StrMJD: Double,  gsicsCorr_EndMJD: Double,  gsicsUpperLimit: Float,  gsicsLowerLimit: Float, /* - ,12 , 12 */
  gsicsFilename: String,  spare: String) extends InterCalib_Info{
  val gsicsBias : Double= Double.NaN
  val gsicsUncert : Double= Double.NaN
  val gsicsStscene: Double = Double.NaN
  val gsicsCorrInfo=""
}
object InterCalib_Info_11 extends Hisd {
  def apply(raf: RandomAccessFile): InterCalib_Info_11 = {
    InterCalib_Info_11(u1(raf), u2(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f4(raf), f4(raf),
      s(raf, 128), s(raf, 128))
  }
}

case class InterCalib_Info_12
( HeaderNum: Int,  BlockLen: Int,  gsicsCorr_C: Double,  gsicsCorr_1: Double, /* 5 , 5 , 4  */
  gsicsCorr_2: Double,  gsicsBias: Double,  gsicsUncert: Double,  gsicsStscene: Double, /* - , - , 8  */
  gsicsCorr_StrMJD: Double,  gsicsCorr_EndMJD: Double,  gsicsUpperLimit: Float,  gsicsLowerLimit: Float, /* - ,12 , 12 */
  gsicsFilename: String,  spare: String) extends InterCalib_Info{
  val gsicsCorr_C_er: Double=Double.NaN
  val gsicsCorr_1_er: Double=Double.NaN
  val gsicsCorr_2_er: Double=Double.NaN
  val gsicsCorrInfo=""
}

object InterCalib_Info_12 extends Hisd {
  def apply(raf: RandomAccessFile): InterCalib_Info_12 = {
    InterCalib_Info_12(u1(raf), u2(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f8(raf), f8(raf),
      f8(raf), f8(raf), f4(raf), f4(raf),
      s(raf, 128), s(raf, 56))
  }
}

/* #7 */
case class Segm_Info
( HeaderNum: Int,  BlockLen: Int,  totalSegNum: Int,  segSeqNo: Int, /* 4 */
  strLineNo: Int,  spare: String)

object Segm_Info extends Hisd {
  def apply(raf: RandomAccessFile): Segm_Info = {
    Segm_Info(u1(raf), u2(raf), u1(raf), u1(raf),
      u2(raf), s(raf, 40))
  }
}

/* #8 */
case class NaviCorr_Info
( HeaderNum: Int,  BlockLen: Int,  RoCenterColumn: Float,  RoCenterLine: Float, /* 4 */
  RoCorrection: Double,  correctNum: Int,  lineNo: Array[Int],  columnShift: Array[Float], /* 8 */
  lineShift: Array[Float],  spare: String)

object NaviCorr_Info extends Hisd {
  def apply(raf: RandomAccessFile): NaviCorr_Info = {
    val HeaderNum = u1(raf)
    val BlockLen = u2(raf)
    val RoCenterColumn = f4(raf)
    val RoCenterLine = f4(raf)
    val RoCorrection = f8(raf)
    val correctNum = u2(raf)
    val lineNos = ListBuffer[Int]()
    val columnShifts = ListBuffer[Float]()
    val lineShifts = ListBuffer[Float]()
    for (_ <- 0 until correctNum) {
      lineNos += u2(raf)
      columnShifts += f4(raf)
      lineShifts += f4(raf)
    }
    val spare = s(raf, 40)
    NaviCorr_Info(HeaderNum, BlockLen, RoCenterColumn, RoCenterLine, RoCorrection, correctNum,
      lineNos.toArray, columnShifts.toArray, lineShifts.toArray, spare)
  }
}

/* #9 */
case class ObsTime_Info
( HeaderNum: Int,  BlockLen: Int,  obsNum: Int,  lineNo: Array[Int],
  obsMJD: Array[Double],  spare: String)

object ObsTime_Info extends Hisd {
  def apply(raf: RandomAccessFile): ObsTime_Info = {
    val HeaderNum = u1(raf)
    val BlockLen = u2(raf)
    val obsNum = u2(raf)
    val lineNos = ListBuffer[Int]()
    val obsMJD = ListBuffer[Double]()
    for (_ <- 0 until obsNum) {
      lineNos += u2(raf)
      obsMJD += f8(raf)
    }
    val spare = s(raf, 40)
    ObsTime_Info(HeaderNum, BlockLen, obsNum, lineNos.toArray, obsMJD.toArray, spare)
  }
}

/* #10 */
case class Error_Info
( HeaderNum: Int,  BlockLen: Long,  errorNum: Int,  lineNo: Array[Int],
  errPixNum: Array[Int],  spare: String)

object Error_Info extends Hisd {
  def apply(raf: RandomAccessFile): Error_Info = {
    val HeaderNum = u1(raf)
    val BlockLen = u4(raf)
    val errorNum = u2(raf)
    val lineNos = ListBuffer[Int]()
    val errPixNum = ListBuffer[Int]()
    for (_ <- 0 until errorNum) {
      lineNos += u2(raf)
      errPixNum += u2(raf)
    }
    val spare = s(raf, 40)
    Error_Info(HeaderNum, BlockLen, errorNum, lineNos.toArray, errPixNum.toArray, spare)
  }
}

/* #11 */
case class Spare( HeaderNum: Int,  BlockLen: Int,  spare: String)

object Spare extends Hisd {
  def apply(raf: RandomAccessFile): Spare = {
    Spare(u1(raf), u2(raf), s(raf, 256))
  }
}

case class Correct_Table
( flag: Char,  startLineNo: Int,  lineNum: Int,
  cmpCoff: Array[Float],  cmpLoff: Array[Float])

/* #12 */
object CData extends Hisd {
  def apply(raf: RandomAccessFile, nLin:Int, nPix:Int):Array[Array[Short]] = {
    i2(raf,nLin,nPix)
  }
}