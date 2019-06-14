package stat

import common.BaseType.AF2
import common.MyUtils._

/**
  * Created by北京， 何险峰，胡骏楠，黄琰 on 16-5-23.
  */
object StaDensites {
  val leftLat = 54
  val leftLon = 73
  val rightLat = 18
  val rightLon = 136
  val nlat = leftLat - rightLat + 1
  val nlon = rightLon - leftLon + 1
  val density6Fnm = "./doc/knn/density.txt"
  val dens6Mat : AF2 =  fileLines(density6Fnm)
    .map(f => f.split(",").map(g => g.toFloat) )
  val threshold =  0.004f

  val densityAFnm = "./doc/knn/densityA.txt"
  val densAMat : AF2 =  fileLines(densityAFnm)
    .map(f => f.split(",").map(g => g.toFloat) )

  def needVirt(lat : Float,lon:Float):Boolean={
    val i = (leftLat-lat).toInt
    val j = (lon - leftLon).toInt
    dens6Mat(i)(j) < threshold
  }

  def fetchProbability(lat : Float,lon:Float):Float={
    val i = (leftLat-lat).toInt
    val j = (lon - leftLon).toInt
    try {
      densAMat(i)(j)
    } catch {
      case e:Exception => 0.0001f
    }
  }

  def main(args: Array[String]) {

  }
}
