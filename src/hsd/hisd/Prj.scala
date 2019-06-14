package hsd.hisd

import scala.math._
import HsdConfig._
/**
 * Created by 何险峰，成都 on 2015/10/6.
 */

case class Prj(pix : Array[Array[Short]], lin : Array[Array[Short]])

object Prj  {
  import DataType._
  case class PixLin(pix:Short, lin:Short)
  //#经纬度转换为行列号
  def apply(head : Head):Prj={
    val matPix : AS2 = Array.fill[Short](height,width)(0)
    val matLin : AS2 = Array.fill[Short](height,width)(0)
    for(
      i <- (0 until height).par;
      lat = lats(i);
      j <-0 until width;
      lon = lons(j)){
      val pl = lonlat_to_pixlin(head,lat,lon)
      matPix(i)(j)=pl.pix
      matLin(i)(j)=pl.lin
    }
    Prj(matPix,matLin)
  }
  private def lonlat_to_pixlin(head: Head, lat0: Float, lon0: Float):PixLin = {
    import head.proj._
    val SCLUNIT = 1.525878906250000e-05

    val lat = toRadians(lat0)
    val lon = toRadians(lon0 -subLon)

    val phi     = atan(projParam2 * tan(lat))
    val cosPhi  = cos(phi)
    val sinPhi  = sin(phi)
    val cosPhi2 = cosPhi * cosPhi
    val cosLon  = cos(lon)
    val sinLon  = sin(lon)

    val Re = polrRadius / sqrt(1.0f - projParam1 * cosPhi2)
    val ReCosPhi = Re * cosPhi
    val ReCosPhiCos_lon = ReCosPhi * cosLon

    val r1 = satDis - ReCosPhiCos_lon
    val r2 = -ReCosPhi * sinLon
    val r3 = Re * sinPhi
    val vx = ReCosPhiCos_lon
    val checkv = -r1 * vx - r2 * r2 + r3 * r3
    if (checkv > 0) println(s"checkv=${checkv}>0")
    val rn = sqrt(r1 * r1 + r2 * r2 + r3 * r3)
    val x = toDegrees(atan2(-r2, r1))
    val y = toDegrees(asin(-r3 / rn))
    val pix = (coff + x * SCLUNIT * cfac).toShort
    val lin = (loff + y * SCLUNIT * lfac).toShort
    PixLin(pix,lin)
  }
  def main(args: Array[String]): Unit = {
    test
  }
  def test: Unit ={
    val swathFnms = Array(
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0110.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0210.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0310.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0410.DAT",
      "./doc/HS_H08_20150914_0630_B01_FLDK_R10_S0510.DAT"
    )
    val pf = Profile(swathFnms)
    prn("pix-----", pf.pixLin.pix)
    prn("lin-----",pf.pixLin.lin)
    val lmin = pf.pixLin.lin.flatten.min
    val lmax = pf.pixLin.lin.flatten.max
    val head = pf.heads(0)
    val sLine = head.seg.strLineNo
    val eLine = sLine + head.data.nLin - 1
  }
  def prn(name:String,mat : AS2): Unit ={
    println(name)
    mat.slice(0,5).map(f=>println(f.slice(0,5).mkString(",")))
  }
}
