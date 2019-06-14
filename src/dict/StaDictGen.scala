package dict

import common.BaseType.{StaInfo, StaMap}
import common.MyConfig._
import common.MyUtils.wrt2txt
import grid.Dem
import station.cmiss.{CmEleH, EleH}
import scala.collection.concurrent.TrieMap
import scala.io.Source

/**
  * 根据地面观测记录，产生要素站点字典
  * Created by 何险峰，维也纳 on 2017/4/2.
  */
object StaDictGen extends Dict {
  //val state = "ext"
  def main(args: Array[String]): Unit = {
    val fromYmdhm="201809240000"
    val toYmdhm  ="201809250000"
    val noWrt = false
    mkDicktsByHours(fromYmdhm,toYmdhm,noWrt)
  }

  def mkDicktsByHours(fromYmdhm : String,toYmdhm:String,noWrt:Boolean = false): Unit ={
    val isExt = toYmdhm > fromYmdhm
    var ymdhm = fromYmdhm
    var dictMapOld = StaDict.dictMap
    while (ymdhm <= toYmdhm){
      dictMapOld = mkDicts(dictMapOld,ymdhm,noWrt,isExt)
      ymdhm = common.TimeTransform.nextYmdhm_hour(ymdhm,1)
    }
  }

  /**
    * 根据样列产生气压，湿度，风，气温字典csv文件
    * 注意：csv文件中，5xxxx站点需要手动更新
    */
  def mkDicts(dictMapOld : TrieMap[String,StaMap],ymdhm:String,noWrt : Boolean,isExt : Boolean): TrieMap[String, StaMap] ={
    val elehArr: Array[EleH] = CmEleH.getEleXArr("EleH",ymdhm).toArray
    println(s"eleh.size : ${elehArr.length}")
    val trie = TrieMap[String, StaMap]()
    dictMapOld.par.foreach{ f =>
      val dictNm = f._1
      val staMap = f._2
      //将要素站点字典名称转换为要素名
      val eleNm = dictFnm2EleNm(dictNm)
      val a_elehArr: Array[EleH] = if (eleNm.length > 0) {
        if(eleNm=="PRE_1h")
          elehArr
        else
          elehArr.filter(eleh => !getVal(eleh, EleNms.indexOf(eleNm)).isNaN)
      } else if (dictNm == dict3Fnm) {
        elehArr.filter { eleh =>
          val sta = eleh.sta
          val v = getVal(eleh, EleNms.indexOf("TEM"))
          !v.isNaN && (sta <= 59999) && (sta >= 50000)
        }
      } else if (dictNm == dictRFnm) {
        elehArr.filter(eleh => !eleh.alt.isNaN)
      } else Array[EleH]()

      val staMapNew =  if (a_elehArr.isEmpty)
        TrieMap[Int, StaInfo]()
      else  if (a_elehArr.length > 0 && isExt)
        mkStaMapExt(staMap,a_elehArr, dictNm,noWrt)
      else
        mkStaMap(a_elehArr, dictNm,noWrt)

      trie += ((eleNm , staMapNew))
    }
    if (!noWrt) mkDictA()
    trie
  }

  private def mkStaMap(eleh: Array[EleH], fnm: String,noWrt:Boolean): TrieMap[Int, StaInfo] = {
    val staInfos: Array[StaInfo] = getStaInfos(eleh)
    val trie = TrieMap[Int, StaInfo]()
    staInfos.par.foreach(s => trie += (( s.sta, s)))
    mkTxtFile(staInfos,fnm,noWrt)
    trie
  }

  private def mkTxtFile(staInfos : Array[StaInfo], fnm: String,noWrt:Boolean ){
    println(fnm, "length=>", staInfos.length)
    if (!noWrt) {
      val csvFnm = getDictFnm(fnm)
      val str = staInfos.map(f => s"${f.sta},${f.lat},${f.lon},${f.alt}")
        .sorted
        .mkString("\n")
        .trim
      common.MyUtils.wrt2txt(csvFnm, str)
    }
  }

  private def mkStaMapExt(staMapOld : StaMap,a_eleh: Array[EleH], fnm: String,noWrt:Boolean) : StaMap={
    val staInfos = getStaInfos(a_eleh)
    val trieCur = TrieMap[Int, StaInfo]()
    staInfos.par.foreach(s => trieCur += (( s.sta, s)))
    trieCur ++= staMapOld

    val staInfos_ext: Array[StaInfo] = trieCur.values.toArray
    mkTxtFile(staInfos_ext,fnm,noWrt)
    trieCur
  }

  private def getStaInfos(a_eleh: Array[EleH]): Array[StaInfo] ={
    val staInfos = a_eleh.map { f =>
      //尽量使用站点的高度信息，DEM高度作为补充
      val altSta = f.sta
      val altDem: Float = Dem.getAlt(f.lat, f.lon)
      val alt: Float = if (altDem.isNaN) altSta
      else if (f.sta < 59999) f.alt
      else if (altSta > 0 && altSta < 4500) f.alt
      else altDem
      StaInfo(f.sta, f.lat, f.lon, alt)
    }
    staInfos
  }
  private def mkDictA(): Unit = {
    val dictRTxt = Source.fromFile(getDictFnm(dictRFnm)).getLines().mkString("\n")
    val dictVTxt = Source.fromFile(getDictFnm(dictVFnm)).getLines().mkString("\n")
    val dictATxt = s"$dictRTxt\n$dictVTxt"
    wrt2txt(getDictFnm(dictAFnm), dictATxt)
  }
}
