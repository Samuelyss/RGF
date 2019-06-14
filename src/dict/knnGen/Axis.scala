package dict.knnGen

import common.BaseType.{SMDI,StaMap}
import dict._
import scala.collection.immutable.SortedMap
import scala.util.Random
/**
  * 目的：建立经度，纬度方向站点轴
  * Created by 何险峰，维也纳 on 2017/4/25.
  */

object LatLonAxis {
  val rand = Random
  def apply(dictFnm : String) : (Axis,Axis) = {
    val dict = StaDict.fetchDict(dictFnm)
    val lat_sta : SMDI = mkAxis(dict,isLat = true)
    val lon_sta : SMDI = mkAxis(dict,isLat = false)
    (Axis(lat_sta),Axis(lon_sta))
  }
  private def mkAxis(dict : StaMap,isLat : Boolean) : SMDI ={
    val d2i : SMDI = SortedMap.empty[Double, Int] ++ dict.par.map{f =>
      val sta = f._1
      val lv = if (isLat) f._2.lat else f._2.lon
      val key = mkFloatKey(lv)
      key -> sta
    }
    d2i
  }
  private def mkFloatKey(f : Double):Double={
    f + rand.nextDouble() / 100000
  }
}

case class Axis(fs : SMDI) {
  // f: 经度或纬度，s：站号
  private val delta = 0.0001f
  private val mn = fs.keySet.min - delta
  private val mx = fs.keySet.max + delta

  private def getNearStas(c : Float, radius:Float):Set[Int]={
    val a : Float = c - radius
    val b : Float = c + radius
    val cmin = if (a < mn ) mn else a
    val cmax = if (b > mx ) mx else b
    val ia = fs.to(cmin).size - 1
    val ib = fs.to(cmax).size
    val stas = fs.values.slice(ia,ib).toSet[Int]
    stas
  }

  def fetchStas(c : Float,radius : Float,step:Float,k:Int):Set[Int]={
    val stas = getNearStas(c,radius)
    if (stas.size >= k) stas else fetchStas(c,radius + step,step,k)
  }
}
