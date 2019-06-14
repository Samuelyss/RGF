package dict

import common.BaseType.{StaInfo, StaMap}
import common.MyUtils.fileLines
import common.MyConfig._
import grid.Dem
import scala.collection.concurrent.TrieMap

/**
  * Created by 何险峰，维也纳  on 2017/4/3.
  */
object StaDict extends Dict {
  lazy val isLoaded = dictMap.size > 0
  val dictMap: TrieMap[String, StaMap] = loadStaMaps
  val dictStaAll: StaMap = dictMap(dictAFnm)
  val dictV: StaMap = dictMap(dictVFnm)
  lazy val dictGrid: StaMap = dictMap(dictGridFnm)
  lazy val dictGridIJ: TrieMap[Int, (Int, Int)] = dictGrid2ij(dictGrid)

  def main(args: Array[String]): Unit = {
    println("test")
  }
  //有可能是要素，所有，虚拟，检验，格点站点字典
  def fetchDict(dictNm: String): StaMap = {
    val nm = if (dictNm.contains("dictGrid"))
      dictGridFnm
    else
      dictNm
    if (dictNms.contains(nm))
      dictMap(nm)
    else
      throw new Exception(s"$dictNm not exists in dictNms !")
  }

  def switchDict(elehNm: String): StaMap = {
    val dictNm = switchDictNm(elehNm)
    fetchDict(dictNm)
  }

  def switchDict(elehIdx: Int): StaMap = {
    val elehNm = EleNms(elehIdx)
    switchDict(elehNm)
  }

  def loadStaMaps: TrieMap[String, StaMap] = {
    val trie = TrieMap[String, StaMap]()
    dictNms.foreach { dictNm =>
      val ds = loadDict(dictNm)
      trie += ((dictNm, ds))
    }
    trie
  }

  def loadDict(fnm: String): StaMap = {
    val trie = TrieMap[Int, StaInfo]()
    val lines = fileLines(getDictFnm(fnm))
    lines.par.foreach{str =>
      val s = str.split(",").map(_.trim)
      val sta = s(0).toInt
      val lat = s(1).toFloat
      val lon = s(2).toFloat
      val alt = Dem.getAlt(lat, lon)
      val si = StaInfo(sta, lat, lon, alt)
      trie += ((sta, si))
    }
    println(s"字典$fnm 站点:${trie.size}")
    trie
  }

  private def dictGrid2ij(dictGd: StaMap): TrieMap[Int, (Int, Int)] = {
    val trie = TrieMap[Int, (Int, Int)]()
    dictGd.keySet.par.foreach{ z =>
      val (i, j) = z2ij(z)
      trie +=((z, (i, j)))
    }
    trie
  }
}
