package dict

import common.BaseType.{StaInfo, StaMap}
import dict.StaDictGen.mkDicts
import scala.collection.concurrent.TrieMap
/**
  * Created by 何险峰，成都  on 2017/9/16.
  */

object StaDictGenNew extends Dict {
  def main(args: Array[String]): Unit = {
    val fromYmdhm = "201708011900"
    val toYmdhm = "201708131400"
    val noWrt = false
    mkDicktsByHours(fromYmdhm, toYmdhm, noWrt)
  }

  def mkDicktsByHours(fromYmdhm: String, toYmdhm: String, noWrt: Boolean = false) = {
    var dictMapOld = init

    val isExt = toYmdhm > fromYmdhm
    var ymdhm = fromYmdhm
    while (ymdhm <= toYmdhm) {
      dictMapOld = mkDicts(dictMapOld, ymdhm, noWrt, isExt)
      ymdhm = common.TimeTransform.nextYmdhm_hour(ymdhm, 1)
    }
  }

  def init: TrieMap[String, StaMap] = {
    val trie = TrieMap[String,StaMap]()
    dictNms.indices.foreach{ i =>
      val dictNm = dictNms(i)
      val ds = TrieMap[Int,StaInfo]()
      trie +=((dictNm,ds))
    }
    trie
  }
}
