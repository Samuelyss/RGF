package hsd.nc

import hsd.hisd.Phy
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import ucar.ma2.{ArrayDouble, ArrayFloat, ArrayShort, DataType}
import ucar.nc2._
import ucar.nc2.constants.CDM
import hsd.hisd.HsdConfig._
import hsd.hisd.DataType._

/**
  * Created by 何险峰 on 15-12-1.
  */
object HimaNc {
  def apply(ofn: String, ymdhm: String, phys : Array[Phy]): Unit ={
//    println(s"[HimaNc in 0] $ofn $ymdhm ")
    val himaNc = if (phys.length == 1)
      new SingleBandNC(ofn,ymdhm,phys)
    else
      new MutiBandNc(ofn,ymdhm,phys)
//    println(s"[HimaNc in 1] $ofn $ymdhm ")
    himaNc.mkNc4()
//    println(s"[HimaNc out] $ofn $ymdhm ")
  }
}
trait HimaNc {
  val ofn: String
  val ymdhm: String
  val phys : Array[Phy]
  def mkNc4()
  def ymdhm2y_m_d_hm(ymdhmStr: String):String={
    val y_m_d_hms_fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:00")
    val datetime = DateTime.parse(ymdhmStr, DateTimeFormat.forPattern("yyyyMMddHHmm"))
    val y_m_d_hmStr = y_m_d_hms_fmt.print(datetime)
    y_m_d_hmStr
  }

  val version = NetcdfFileWriter.Version.netcdf4
  val writer: NetcdfFileWriter = NetcdfFileWriter.createNew(version, ofn, null)
  val root: Group = writer.addGroup(null, "/")
  addAttrs(root, List(
    ("time_start", s"$ymdhm"),
    (CDM.DESCRIPTION, "Himawari-8 products"),
    (CDM.HISTORY, s"Created $ymdhm"),
    (CDM.TITLE, s"${ymdhm}PWSC of CMA"),
    ("dataType", "short interger")
  ))
  addAttrNum(root, "centerLat", ltlat - dlat * height / 2)
  addAttrNum(root, "centerLon", ltlon - dlon * width / 2)
  addAttrNum(root, "lowerLeftLat", ltlat - dlat * height)
  addAttrNum(root, "lowerLeftLon", ltlon)
  addAttrNum(root, "gridsize", dlat)
  addAttrNum(root, "dataType", dlat)
  val nTime = 1
  val dims3 = new java.util.ArrayList[Dimension]
  val timeDim: Dimension = writer.addDimension(root, "time", nTime)
  val latDim: Dimension = writer.addDimension(root, "lat", height)
  val lonDim: Dimension = writer.addDimension(root, "lon", width)
  dims3.add(timeDim)
  dims3.add(latDim)
  dims3.add(lonDim)

  val time: Variable = writer.addVariable(root, "time", DataType.DOUBLE, "time")
  time.addAttribute(new Attribute("calendar", "gregorian"))
  time.addAttribute(new Attribute(CDM.UNITS, s"seconds since ${ymdhm2y_m_d_hm(ymdhm)}"))
  val timeVal = new ArrayDouble.D1(1)
  timeVal.set(0, 0)
  //timeVal.set(0, getHours(ymdhm))

  val latitude: Variable = writer.addVariable(root, "lat", DataType.FLOAT, "lat")
  latitude.addAttribute(new Attribute(constants.CDM.UNITS, "degree"))
  val latVal = new ArrayFloat.D1(height)
  for (i <- 0 until height)
    latVal.set(i, ltlat - i * dlat)

  val longitude: Variable = writer.addVariable(root, "lon", DataType.FLOAT, "lon")
  longitude.addAttribute(new Attribute(CDM.UNITS, "degree"))
  val lonVal = new ArrayFloat.D1(width)
  for (i <- 0 until width)
    lonVal.set(i, ltlon + i * dlon)

  case class VarValPair(var0: Variable, val0: ArrayShort.D3)

  def mkAlbedo(radiance : AF2,band : Int): VarValPair = {
    val i = band-1
    val albedo: Variable = writer.addVariable(root, s"B${bands(i).n}", DataType.SHORT, dims3)
    addAttr(albedo, s"B${bands(i).n}", "%", "time lat lon", setFRange(0.0f, 105.0f),1f/10000f,0.0f)
    val albedoVal: ArrayShort.D3 = new ArrayShort.D3(nTime,height, width)
    val val0 = mkGd3(albedoVal, radiance,isTem = false)
    VarValPair(albedo, val0)
  }

  def mkTemp(radiance : AF2,band : Int): VarValPair = {
    val i = band-1
    val temperature: Variable = writer.addVariable(root, s"B${bands(i).n}", DataType.SHORT, dims3)
    addAttr(temperature, s"B${bands(i).n}", "K", "time lat lon", setFRange(100.0f, 400.0f),0.01f, 273.15f)
    val temperatureVal = new ArrayShort.D3(nTime,height, width)
    val val0 = mkGd3(temperatureVal, radiance,isTem = true)
    VarValPair(temperature, val0)
  }

  def addAttrNum(root: Group, attNm: String, v: Number): Unit = {
    root.addAttribute(new Attribute(attNm, v))
  }

  def addAttrs(root: Group, attrPair: List[(String, String)]) {
    attrPair.foreach(f =>
      root.addAttribute(new Attribute(f._1, f._2))
    )
  }

  def addAttr(v: Variable, name: String, unit: String,
              cordinate: String = "time lat lon", range: ArrayFloat.D1,
             scale_factor : Float,add_offset : Float ) {
    v.addAttribute(new Attribute(CDM.LONG_NAME, name))
    v.addAttribute(new Attribute(CDM.UNITS, unit))
    v.addAttribute(new Attribute(CDM.MISSING_VALUE, -9999))
    v.addAttribute(new Attribute(CDM.SCALE_FACTOR, scale_factor))
    v.addAttribute(new Attribute(CDM.ADD_OFFSET, add_offset))
    v.addAttribute(new Attribute(constants._Coordinate.Axes, cordinate))
  }

  def setFRange(mn: Float, mx: Float): ucar.ma2.ArrayFloat.D1 = {
    val fs = new ArrayFloat.D1(2)
    fs.set(0, mn)
    fs.set(1, mx)
    fs
  }

  def mkGd3(gd3: ucar.ma2.ArrayShort.D3, radiance: AF2, isTem : Boolean): ucar.ma2.ArrayShort.D3 = {
    val ti = 0
    val rows = radiance.length
    val cols = radiance(0).length
    for (
      i <- 0 until rows;
      j <- 0 until cols) {
      val v = if (isTem)
        ((radiance(i)(j) - 273.15f) * 100).toShort
      else
        (radiance(i)(j) * 10000).toShort
      gd3.set(ti,i, j, v)
    }
    gd3
  }
}
