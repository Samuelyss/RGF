package dict

import common.BaseType.{StaWD, StaWDMap, StawdArr}
import common.MyUtils.fileLines
import common.MyConfig._
import scala.collection.concurrent.TrieMap
/**
  * Created by 何险峰，维也纳 on 2017/4/1.
  */
object StaNear extends Dict{
  lazy val isLoaded = staNears.size > 0
  val staNears : TrieMap[String,StaWDMap] =  loadStaNears

  def main(args: Array[String]): Unit = {
    staNears.foreach(f => println("near name:",f._1))
  }

  def switchNear(elehNm: String): StaWDMap = {
    val dictNm = switchDictNm(elehNm)
    fetchStaNear(dictNm)
  }

  def switchNear(elehIdx: Int): StaWDMap = {
    val elehNm = EleNms(elehIdx)
    switchNear(elehNm)
  }

  def switchNear(eleIdx: Int,nIter:Int): StaWDMap = {
    nIter match {
      case 0 | 1 => switchNear(eleIdx)
      case _ => fetchStaNear(dict3Fnm)
        /*
      case 2 => fetchStaNear(dict6Fnm)
      case 3 => fetchStaNear(dictTEMFnm)
      case 4 => fetchStaNear(dictRHUFnm)
      case 5 => fetchStaNear(dictWindFnm)
      case 6 => fetchStaNear(dictPRSFnm)
      case 7 => fetchStaNear(dictVISMFnm)
      case 8 => fetchStaNear(dictT0Fnm)
      case 9 => fetchStaNear(dict3Fnm)
      case _ => fetchStaNear(dictAFnm)
      */
    }
  }

  def fetchStaNear(dictNm : String): StaWDMap = {
    if (dictNearNmsExt.contains(dictNm))
      staNears(dictNm)
    else
      throw new Exception(s"$dictNm not exists in dictNms !")
  }

  def loadStaNears : TrieMap[String,StaWDMap] ={
    val trie = TrieMap[String,StaWDMap]()
    dictNearNmsExt.foreach{dictNm =>
      val staWdMap = loadKnnStaWD(dictNm)
      trie +=((dictNm, staWdMap))
    }
    trie
  }
  /**
    * 根据站点字典文件名,读入最邻近站点权值集合
    * @return
    */
  def loadKnnStaWD(dictFnm:String):StaWDMap={
    println(s"加载$dictFnm 因子图...")
    val nearStaFnm : String = getNearStaWDZFnm(dictFnm)
    val trie = TrieMap[Int,StawdArr]()
    val lines = fileLines(nearStaFnm)
    lines.par.foreach{str =>
      val stawdArr = str.split(",")
        .grouped(4)
        .map(f => StaWD(f(0).toInt, f(1).toFloat, f(2).toFloat, f(3).toFloat))
        .toArray
      val sta = stawdArr(0).sta
      //0: 站点自己，1..k : 邻近站
      trie +=((sta,stawdArr.slice(1,stawdArr.length)))
    }
    trie
  }
}
