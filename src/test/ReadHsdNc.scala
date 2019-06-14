package test

import common.MyUtils._
import ucar.ma2.ArrayFloat
import ucar.nc2.dataset.NetcdfDataset
/**
  * Created by hxf on 16-6-28.
  */
object ReadHsdNc {
  def main(args: Array[String]) {
    val ncFnm ="/home/hxf/hsd/h8_nc/2016/20160628/HS_H08_20160628_0010_B01_FLDK_R10_S0110.nc"
    val nc = NetcdfDataset.openDataset(ncFnm)
    val varNm="B01"
    val v = nc.findVariable(varNm)
    val vb2 = v.read.asInstanceOf[ArrayFloat.D2]
    val shp = vb2.getShape
    val nLat = shp(0)
    val nLon = shp(1)
    println(s"nLat=${nLat}, nLon=${nLon}")
    val mat = Array.fill[Float](shp(0), shp(1))(Float.NaN)
    for (i <- (0 until nLat).par;
         j <- (0 until nLon).par) {
      val v = vb2.get(i, j)
      if (!v.isNaN || v > -999) {
        mat(i)(j) = v
      }
    }
    nc.close()
  }
}
