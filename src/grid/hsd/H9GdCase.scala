package grid.hsd

import common.BaseType._
import common.MakePng
import common.MyUtils._

/**
  * Created by 何险峰，北京 on 16-6-28.
  */
case class H9GdCase(lats: SMFI, lons: SMFI, gd: AF2) {
  def atLatLon(lat: Float, lon: Float): Float = {
    val is = lats.to(lat)
    val i = if (is.nonEmpty) is.last._2 else lats(lats.keySet.firstKey)
    val js = lons.to(lon)
    val j = if(js.nonEmpty) js.last._2 else lons(lons.keySet.firstKey)
    gd(i)(j)
  }

  def mkPng4test() = {
    val nLat = gd.length
    val nLon = gd(0).length
    val arr = Array.fill[Short](nLat, nLon)(-9999)
    lats.keySet.foreach { lat =>
      val i = lats(lat)
      lons.keySet.foreach { lon =>
        val j = lons(lon)
        val v = atLatLon(lat, lon)
        val v1 = if (math.abs(v) < 1) v * 100 else v
        if (!v.isNaN) arr(i)(j) = v1.toShort
      }
    }
    MakePng.mkPng(arr, "./doc/tem.png", MakePng.ModelType.Normal)
  }
}

