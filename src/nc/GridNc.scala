package nc

import java.io.File
import java.util

import common.BaseType._
import common.TimeTransform._
import grid.Dem._
import ucar.ma2.{ArrayByte, ArrayDouble, ArrayFloat, DataType}
import ucar.nc2.constants.{CDM, _Coordinate}
import ucar.nc2.write.Nc4ChunkingStrategy
import ucar.nc2.{Attribute, Dimension, Group, NetcdfFileWriter, Variable}
import scala.concurrent._
/**
  * nc格点场输出接口
  * Created by 何险峰，北京 on 16-6-19.
  */
trait GridNc {
  case class Head0(writer: NetcdfFileWriter, root: Group,dims3: util.ArrayList[Dimension],
                   time: Variable, timeVal: ArrayDouble.D1,
                   latitude: Variable, latlVal: ArrayDouble.D1,
                   longitude: Variable, lonlVal: ArrayDouble.D1, nTime:Int)
  //def batchApply(fromYmdhm : String,toYmdhm: String,isOverWrite : Boolean=true, isForecast : Boolean = false)
  def doMkNc4(ymdhm : String, gd3 : AS3,ncFnm : String)
  def mkNc4(ymdhm : String, gd3 : AS3, nf:Int): Unit = {
    val ncPath = getNcPath(ymdhm,nf)
    doMkNc4(ymdhm,gd3,ncPath)
  }
  def chkNcExists(ymdhm: String,nf:Int):Boolean={
    val fnm = getNcPath(ymdhm,nf)
    val f = new File(fnm)
    ( f.exists() && f.length() > 100 * 1024 )
  }
  def getNcPath(ymdhm: String, nf:Int): String = {
    val year = getYear(ymdhm)
    val ymd = getYmd(ymdhm)
    val dir0 = common.MyConfig.getDataDir(ymdhm)
    val dir1 = s"${dir0.outNcDir}/eleh"
    val dir2 = s"$dir1/$year/$ymd"
    new File(dir2).mkdirs
    val fore_minus = "%05d".format(nf * 10)
    s"$dir2/MSP1_PMSC_ELEH_ME_L88_CHN_${ymdhm}_$fore_minus-00000.nc"
  }

  def setBRange(mn: Byte, mx: Byte): ArrayByte.D1 = {
    val bs = new ArrayByte.D1(2)
    bs.set(0, mn)
    bs.set(1, mx)
    bs
  }

  def setFRange(mn: Float, mx: Float): ArrayFloat.D1 = {
    val fs = new ArrayFloat.D1(2)
    fs.set(0, mn)
    fs.set(1, mx)
    fs
  }

  //import ucar.ma2.{ArrayDouble, ArrayFloat, DataType}
  //import ucar.nc2.constants.{CDM, _Coordinate}
  //import ucar.nc2.{Attribute, Dimension,  NetcdfFileWriter, Variable}

  def mkGd3(ei: Int, gd3: AS3,writer : NetcdfFileWriter,var_ei : Variable):Unit= {
    import ucar.ma2
    val tij_gd3 = Array.ofDim[AS2](1)
    tij_gd3(0) = gd3(ei)
    val tij_arr : ma2.Array= ucar.ma2.Array.factory(tij_gd3)
    writer.write(var_ei, tij_arr)
  }
  def addAttr(v: Variable, name: String, unit: String, range: ArrayFloat.D1) {
    val scale_factor = if (name.indexOf("PRE")>=0) 0.01f else 0.1f
    val add_offset = 0.0f
    val cordinate: String = "time,level,lat,lon"
    v.addAttribute(new Attribute(CDM.LONG_NAME, name))
    v.addAttribute(new Attribute(CDM.UNITS, unit))
    v.addAttribute(new Attribute(CDM.MISSING_VALUE, MISSING_SHORT))
    v.addAttribute(new Attribute(CDM.SCALE_FACTOR, scale_factor))
    v.addAttribute(new Attribute(CDM.ADD_OFFSET, add_offset))
    // v.addAttribute(new Attribute(CDM.VALID_RANGE, range))
    v.addAttribute(new Attribute(_Coordinate.Axes, cordinate))
  }

  def mkHead0(ymdhm:String, ncFnm: String, areaNm: String): Head0 = {
    val version = NetcdfFileWriter.Version.netcdf4
    val strategy = ucar.nc2.write.Nc4Chunking.Strategy.standard
    val chunker = Nc4ChunkingStrategy.factory(strategy, 5, true)
    val writer = NetcdfFileWriter.createNew(version, ncFnm, chunker)
    val root: Group = writer.addGroup(null, "/")

    def addAttrs(attrPair: List[(String, String)]) {
      attrPair.foreach(f =>
        root.addAttribute(new Attribute(f._1, f._2))
      )
    }
    def addAttr0(attNm : String,v : Number): Unit ={
      root.addAttribute(new Attribute(attNm,v))
    }

    addAttrs(List(
      (CDM.TITLE, s"${areaNm}ground meteorological grids"),
      (CDM.HISTORY, "observation time : $ymdhm"),
      ("producer", "CMA")
    ))
    addAttr0("upperLeftLat",demLat.maxLat)
    addAttr0("upperLeftLon",demLon.minLon)
    addAttr0("step",demLat.step)

    val nTime = 1
    val dims3 = new util.ArrayList[Dimension]
    val timeDim = writer.addDimension(root, "time", nTime)
    val latDim = writer.addDimension(root, "lat", nLat)
    val lonDim = writer.addDimension(root, "lon", nLon)
    dims3.add(timeDim)
    dims3.add(latDim)
    dims3.add(lonDim)

    val time = writer.addVariable(root, "time", DataType.DOUBLE, "time")
    time.addAttribute(new Attribute("calendar", "gregorian"))
    time.addAttribute(new Attribute(CDM.UNITS, s"seconds since ${common.TimeTransform.ymdhm2y_m_d_hm(ymdhm)}"))
    val timeVal = new ArrayDouble.D1(1)
//    timeVal.set(0, common.TimeTransform.getHours(ymdhm))
    timeVal.set(0, 0)

    val latitude = writer.addVariable(root, "lat", DataType.DOUBLE, "lat")
    latitude.addAttribute(new Attribute(CDM.UNITS, "degrees_north"))
    val latlVal = new ArrayDouble.D1(nLat)
    for (i <- 0 until nLat) latlVal.set(i, demLat.maxLat - i * demLat.step)

    val longitude = writer.addVariable(root, "lon", DataType.DOUBLE, "lon")
    longitude.addAttribute(new Attribute(CDM.UNITS, "degrees_east"))
    val lonlVal = new ArrayDouble.D1(nLon)
    for (i <- 0 until nLon) lonlVal.set(i, demLon.minLon + i * demLon.step)
    Head0(writer, root,dims3,  time, timeVal,  latitude, latlVal, longitude, lonlVal,nTime)
  }

}
