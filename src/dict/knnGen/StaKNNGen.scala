package dict.knnGen

import common.BaseType.{SMFI, StaInfo, StaWD}
import dict.{Dict, StaDict}
import common.MyConfig._
import common.MyUtils._

import scala.collection.concurrent.TrieMap
/**
  * Created by 何险峰，维也纳 on 2017/4/25.
  */
object StaKNNGen extends Dict{
  val isDeg : Boolean = true

  def main(args: Array[String]): Unit = {
    mkOtherKnnFile()
    mkGridKnnFile(dictGridFnm)
    mkGridKnnFile(dictGridExtFnm)
  }
  def mkOtherKnnFile(): Unit ={
    val dstDictFnm = dictAFnm
    dictNearNms.foreach{srcDictFnm =>
      val t0 = System.currentTimeMillis()
      if (! srcDictFnm.contains("dictGrid"))
        mkKnnFile(srcDictFnm,dstDictFnm)
      val t1 = System.currentTimeMillis()
      val ds = (t1 - t0) / 1000.0
      val dm = ds / 60.0
      println(s"take $ds s= $dm m")
    }
  }
  def mkGridKnnFile(gdFnm : String): Unit ={
    val srcDictFnm = dictAFnm
    //val dstDictFnm = dictGridFnm
    val t0 = System.currentTimeMillis()
    mkKnnFile(srcDictFnm,gdFnm)
    val t1 = System.currentTimeMillis()
    val ds = (t1 - t0) / 1000.0
    val dm = ds / 60.0
    println(s"take $ds s= $dm m")
  }

  def mkKnnFile(srcDictFnm : String,dstDictFnm : String){
    println("===========================================")
    println(s"$srcDictFnm - > $dstDictFnm knn")
    val knn = mkKnn(srcDictFnm,dstDictFnm)
//    val isGrid = dstDictFnm.contains("Grid")
    val nearStaWDStr = sta_wds2Txt(knn)
    val knnfnm = if (dstDictFnm.contains("dictGrid"))
      getNearStaWDZFnm(dstDictFnm)
    else
      getNearStaWDZFnm(srcDictFnm)
    wrt2txt(knnfnm, nearStaWDStr)
    println(s"$knnfnm finished!")
  }
  def sta_wds2Txt(staWdmap: TrieMap[Int, Array[StaWD]]): String = {
    val txt: String = staWdmap.par.map { f =>
      val sta = f._1
      val near_stas = f._2
      val str0 = near_stas.map{f =>
        val s = f.sta
        val w = f.w
        val d = f.d
        val dz = f.dz
//        if (isGrid)
//          f"$s,$w%.4f,$d%.4f,$dz%.4f"
//        else
          f"$s,$w%.4f,$d%.4f,$dz%.4f"
      }.mkString(",")
      s"$sta,1.0,0.0,0.0,$str0"
    }.toArray
      .sorted
      .mkString("\n")
    //println(txt)
    txt
  }
  def mkKnn(srcDictFnm : String,dstDictFnm : String): TrieMap[Int, Array[StaWD]] = {
    val trie = TrieMap[Int, Array[StaWD]]()
    val step = 1.0f
    val (latAxis,lonAxis) = LatLonAxis(srcDictFnm)
    val dstDict: TrieMap[Int, StaInfo] = StaDict.fetchDict(dstDictFnm)

    val nlat = latAxis.fs.size
    val nlon = lonAxis.fs.size
    val ndst = dstDict.size

    println(s"$srcDictFnm : nlat=$nlat, nlon = $nlon, $dstDictFnm $ndst ")

    require( nlat <= ndst && nlon <= ndst)

    val (k,initRadius) = srcDictFnm match {
      case `dictAFnm`    =>
        if(dstDictFnm == `dictGridFnm`)
          (numGridNearSta,0.5f)
        else if(dstDictFnm == `dictGridExtFnm`)
          (numGridNearStaExt,1.5f)
        else
          (numNearSta,6.0f)
      case `dict3Fnm`    =>
        (numNearSta,20.0f)
      case `dictVISMFnm`    =>
        (numNearSta,25.0f)
      case _             =>
        (numNearSta,10.0f)
    }

    var nabsent = 0
    dstDict.par.foreach{f =>
      val sta = f._1
      val sa = f._2

      val latNears : Set[Int] = latAxis.fetchStas(sa.lat,initRadius,step,k)
      val lonNears : Set[Int] = lonAxis.fetchStas(sa.lon,initRadius,step,k)

      val intersect = latNears.intersect(lonNears) - sta

      val neighbor_stas : Set[Int] = if (intersect.size < k) {
        nabsent += 1
        val u = latNears ++ lonNears
        println(s"k:$k,nabsent：$nabsent -> nintersect：${intersect.size},nunion:${u.size},$srcDictFnm")
        u
      } else
        intersect

      val calcud = if (isDeg)
        calcuDistDeg(sa) _
      else
        calcuDist(sa) _

      import scala.collection.immutable.SortedMap
      val sta_distances : SMFI = SortedMap.empty[Float, Int] ++ neighbor_stas.map{s =>
        val sb = StaDict.dictStaAll(s)
        val d = calcud(sb)
        d.toFloat -> s
      }.filter(p => p._1 > 1E-3f)

      val knear = sta_distances.toArray.slice(0,k)
      val maxd = knear.last._1
      val R2 = sqr(maxd)
      val calcuW = gaussW(R2) _
//        if (dstDictFnm.contains("dict6") || dstDictFnm.contains("dictGridExt") )
//        gaussWExt(R2) _
//      else
//        gaussW(R2) _
      val swds = knear.map{ds =>
        val d = ds._1
        val sb = ds._2
        val w = calcuW(sqr(d))
        val dz = StaDict.dictStaAll(sb).alt - sa.alt
        StaWD(sb,w,d,dz)
      }
      trie += ((sta,  swds))
    }
    trie
  }
 }
