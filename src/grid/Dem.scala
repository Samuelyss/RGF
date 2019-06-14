package grid

import common.BaseType._
import common.MakePng
import common.MyConfig._
import common.MyUtils._
import stat.StaDensites
import ucar.ma2.ArrayFloat
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset


/**
  * 构建国家3000站,最邻近站点表集合
  * Created by 何险峰,北京 on 15-12-27.
  */
object Dem {
  def main(args: Array[String]) {
    //    val lat = 25.0519f
    //    val lon = 102.5817f
    //    val a = getAlt(lat,lon)
    //    println(a)
        println(s"demLat=$demLat,\n size=${demLat.lats.size}")
        println(s"demLon=$demLon,\n size=${demLon.lons.size}")
    // 除非对系统进行完全初始化，否则请不要使用mkVirtualLocationFile
    // mkVirtualLocationFile
    //prnMat(dem)
  }

  def mkPng(): Unit ={
    val arr=Array.fill[Short](nLat,nLon)(-9999)
    for(i<-0 until nLat;
        j<- 0 until nLon){
      if(!dem(i)(j).equals(Float.NaN))
        arr(i)(j)=dem(i)(j).toShort
    }
    //val arr=dem.map(f=>f.map(f=>f.toShort))
    MakePng.mkPng(arr,"./doc/dem.png",MakePng.ModelType.dem)
  }
  val LatNm = "lat"
  val LonNm = "lon"
  val DemNm = "dem"

  //println(common.MyConfig.demFnm)
  private val nc = NetcdfDataset.openDataset("./doc/dem/G000000_cn1km.nc")
  //val DemNm = data.hour.nc.getVariables.get(0).getFullName
  val dem : AF2 = var2AF2(nc, DemNm)
  val demLat: Latitude = getNcLat(nc)
  val demLon: Longitude = getNcLon(nc)
  val minLat: Float = demLat.minLat
  val maxLat: Float = demLat.maxLat
  val minLon: Float = demLon.minLon
  val maxLon: Float = demLon.maxLon
  val nLat : Int = demLat.nLat
  val nLon : Int = demLon.nLon
  val nLon2 : Int = nLon / 2
  val nLat2 : Int = nLat / 2
  val step : Float = demLat.step

  val maxNumVirSta = 40000
  val vs_base = 910000

  //println(s"maxLat=${maxLat}")

  nc.close()
  //println(s"网格行列:${demLat.nLat} * ${demLon.nLon},格点步长: ${demLat.step.toString} ")

  def getAlt(lat : Float,lon :Float) :Float={
    val lats = demLat.lats
    val lons = demLon.lons
    val nToLat = lats.to(lat).size
    val nToLon = lons.to(lon).size
    val i = if ( nToLat<=0 ) 0 else  nLat - lats.to(lat).last._2 - 1
    val j = if ( nToLon<=0 ) 0 else  lons.to(lon).last._2
    val d = dem(i)(j)
    val d0 = if (d.equals(Float.NaN)) 0.0f  else d//海洋
    //println(s"${lat},${lon}=>(${i},${j})=${d}")
    d0
  }



  /*
  def needv(lat : Float,lon:Float):Boolean={
    val sgema = 8.0
    val s2 = sgema * sgema
    val mLon = 113.0
    val mLat = 32.0
    val dx2 = (lon - mLon) * (lon - mLon)
    val dy2 = (lat - mLat) * (lat - mLat)
    val xy = -(dx2 + dy2) / s2
    val f = math.exp(xy)
    println(f)
    f < 0.45
  }
  */
  /**
    * 从Dem中产生中国西部(nLon/2)虚拟站位置
    */
  def mkVirtualLocationFile(): Unit = {
    var nVir = 0
    println(s"从Dem中产生中国${maxNumVirSta}个虚拟站位置")
    val strB : StringBuilder = new StringBuilder()
    while (nVir < maxNumVirSta ){
      val iLat = randInt(nLat)
      val jLon = randInt(nLon)
      val alt: Float = dem(iLat)(jLon)
      val lon = minLon + jLon * step
      val lat = maxLat - iLat * step
      if (! alt.equals(Float.NaN) && StaDensites.needVirt(lat,lon)) {
        val vsLoc = s"$lat,$lon,$alt\n"
        strB.append(vsLoc)
        nVir += 1
      }
    }

    val vstas = strB
      .toString
      .split("\n")
      .sorted.zipWithIndex
      .map{f =>
        val sta = vs_base + f._2
        s"$sta,${f._1}"
      }.mkString("\n")
    //val dictVFnm ="./doc/knn/dictV.csv"
    wrt2txt(dictVFnm,vstas.toString)
  }


  private def getNcLat(nc: NetcdfFile): Latitude = {
    val v = nc.findVariable(LatNm)
    val arr = v.read.asInstanceOf[ArrayFloat.D1]
    val lats = arr.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    val fi = arr_idx(lats)

    //    val str = fi.map(f => s"(${f._1},${f._2})").mkString(",")
    //    println(str)

    val step = lats(1) - lats(0)
    Latitude(lats(0), lats(lats.length - 1), step, lats.length, fi)
  }

  private def getNcLon(nc: NetcdfFile): Longitude = {
    val v = nc.findVariable(LonNm)
    val arr = v.read.asInstanceOf[ArrayFloat.D1]
    val lons = arr.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    val fi = arr_idx(lons)
    val step = lons(1) - lons(0)
    Longitude(lons(0), lons(lons.length - 1), step, lons.length, fi)
  }


  private def var2AF2(nc: NetcdfFile, varNm: String): AF2 = {
    println("Loading DEM data ...")
    val v = nc.findVariable(varNm)
    val vb2 = v.read.asInstanceOf[ArrayFloat.D2]
    val shp = vb2.getShape
    val nLat = shp(0)
    val nLon = shp(1)
    val demMat = Array.fill[Float](shp(0), shp(1))(Float.NaN)
    for (
      i <- (0 until nLat).par;
      j <- (0 until nLon).par) {
      val alt = vb2.get(i, j)
      demMat(nLat - i - 1)(j) = if (alt < -100f) -100f else if (alt > 7000f) 7000f else alt
    }
    println("DEM data loaded.")
    demMat
  }

  private def prnMat(mat: AF2): Unit = {
    mat.foreach(f => println(f.filter(g => ! g.equals(Float.NaN)) mkString ","))
  }
}
