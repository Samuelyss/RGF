package station.cmiss

import common.TimeTransform.{getMinutes, prevYmdhm_minute}
import scala.collection.concurrent.TrieMap

/**
  * 获取cimiss中综合数据
  * Created by 何险峰，北京 on 17-1-2.
  */
trait CmBd {
  private def getRain(ymdhm: String):(TrieMap[Int,ERain],TrieMap[Int,ERain],TrieMap[Int,ERain])={
    val ymdhm_10m = prevYmdhm_minute(ymdhm, 10)
    val ymdhm_5m = prevYmdhm_minute(ymdhm, 5)

    val rain5mPrevMap = CmERain(ymdhm_10m, ymdhm_5m)
    val rain5mPostMap = CmERain(ymdhm_5m, ymdhm)
    val rain10mMap = CmERain(ymdhm_10m, ymdhm)
    (rain5mPrevMap,rain5mPostMap,rain10mMap)
  }
  private def getCm4Minu(ymdhm:String):(TrieMap[Int, EMinu],TrieMap[Int, EOther],TrieMap[Int, ERain])={
    val ymdhm_60m = prevYmdhm_minute(ymdhm, 60)
    val eminuMap: TrieMap[Int, EMinu] = CmEMinu(ymdhm)
    val eotherMap = CmEOther(ymdhm)
    val rain60mMap = CmERain(ymdhm_60m, ymdhm)
    (eminuMap,eotherMap,rain60mMap)
  }

  def getEleHArr4DictA(ymdhm: String): Array[EleH] = {
    import station.EleChk._

    val minu = getMinutes(ymdhm)
    require(minu % 10 == 0)
    val (rain5mPrevMap,rain5mPostMap,rain10mMap)  = getRain(ymdhm)
    def getPre(sta : Int, pre60m : Float):(Float,Float,Float)={
      val pre10m0 = if (rain10mMap.nonEmpty && rain10mMap.keySet(sta))
        rain10mMap(sta).sumpre else Float.NaN
      val pre5mprev0 = if (rain5mPrevMap.nonEmpty && rain5mPrevMap.keySet(sta))
        rain5mPrevMap(sta).sumpre else Float.NaN
      val pre5mpost0 = if (rain5mPostMap.nonEmpty && rain5mPostMap.keySet(sta))
        rain5mPostMap(sta).sumpre else Float.NaN

      val pre10m = if (!pre60m.isNaN && !pre10m0.isNaN && pre10m0 <= pre60m )
        pre10m0
      else Float.NaN

      val pre5mprev = if (!pre10m.isNaN && !pre5mprev0.isNaN && pre5mprev0 <= pre10m )
        pre5mprev0
      else Float.NaN

      val pre5mpost = if (!pre10m.isNaN && !pre5mpost0.isNaN && pre5mpost0 <= pre10m )
        pre5mpost0
      else Float.NaN
      (pre5mprev,pre5mpost,pre10m)
    }
    val elehArr: Array[EleH] = minu match {
      case 0 =>
        val elehMap: TrieMap[Int, EleH] = CmEleH(ymdhm)
        if(elehMap.isEmpty) Array[EleH]() else
        elehMap.map { f =>
          val sta = f._1
          val e = f._2
          val (pre5mprev,pre5mpost,pre10m) = getPre(sta,e.pre1h)
          EleH(ymdhm, sta, e.lat, e.lon, e.alt, e.prs, e.prsSea,
            e.tem, e.dpt, e.rhu, e.vap, e.pre1h, e.dddmax, e.ffmax,
            e.gst, e.t5cm, e.t10cm, e.t15cm, e.t20cm, e.t40cm, e.u, e.v, e.qse,
            e.gtem, e.vism, e.dddmax, e.ffmax, pre10m,pre5mprev,pre5mpost)
        }.toArray

      case 10 | 20 | 30 | 40 | 50 =>
        val (eminuMap,eotherMap,rain60mMap) = getCm4Minu(ymdhm)
        val mon = common.TimeTransform.getMonth(ymdhm)
        if (eminuMap.isEmpty && eotherMap.isEmpty && rain60mMap.isEmpty)
          Array[EleH]() else {
          eminuMap.map { f =>
            val sta = f._1
            val e = f._2
            val pre60m = if (rain60mMap.nonEmpty && rain60mMap.keySet(sta))
              rain60mMap(sta).sumpre else Float.NaN
            val (pre5mprev,pre5mpost,pre10m) = getPre(sta,pre60m)
            val eo = if (eotherMap.nonEmpty && eotherMap.keySet(sta))
              eotherMap(sta) else null
            val ffmax  = if (eo == null) Float.NaN else eo.ffmax
            val dddmax = if (eo == null) Float.NaN else eo.dddmax
            val u = if (eo == null) Float.NaN else eo.u
            val v = if (eo == null) Float.NaN else eo.v
            val ddd = if (eo == null) Float.NaN else eo.winDAvg2mi
            val ff  = if (eo == null) Float.NaN else eo.winSAvg2mi

            val vism = if (eo == null) Float.NaN else eo.vism

            val vap = if (eo == null) Float.NaN else VAPChk(eo.vap.toString,e.tem,e.rhu)
            val dpt = if (eo == null) Float.NaN else DPTChk(mon,eo.dpt.toString,e.tem, vap)

            val qse = if (eo == null) Float.NaN else calcuQse(e.prs, e.tem, dpt, vap)

            EleH(ymdhm, sta, e.lat, e.lon, e.alt, e.prs, e.prsSea,
              e.tem, dpt, e.rhu, vap, pre60m, ddd, ff,
              e.gst, e.t5cm, e.t10cm, e.t15cm, e.t20cm, e.t40cm, u, v, qse,
              e.gtem, vism, dddmax, ffmax, pre10m,pre5mprev,pre5mpost)
          }.toArray
        }
    }
    elehArr
  }
}
