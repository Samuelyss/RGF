package station.cmiss

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import common.MyUtils._
import java.net.URL

import dict.StaDict

import scala.collection.concurrent.TrieMap
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

/**
  * Created by 何险峰，北京 on 16-7-9.
  */
sealed trait EleX {
  val sta: Int
}

final case class EleH(ymdhm: String, sta: Int, lat: Float, lon: Float, alt: Float,
                      prs: Float, prsSea: Float, tem: Float, dpt: Float, rhu: Float,
                      vap: Float, pre1h: Float, winDAvg2mi: Float, winSAvg2mi: Float,
                      gst: Float, t5cm: Float, t10cm: Float, t15cm: Float, t20cm: Float, t40cm: Float,
                      u: Float, v: Float, qse: Float, gtem: Float, vism: Float,
                      dddmax: Float, ffmax: Float, pre10m: Float, pre5mprev: Float, pre5mpost: Float) extends EleX

final case class ERain(sta: Int, lat: Float, lon: Float, alt: Float, sumpre: Float) extends EleX

final case class EMinu(ymdhm: String, sta: Int, lat: Float, lon: Float, alt: Float,
                       prs: Float, tem: Float, rhu: Float, prsSea: Float,
                       gst: Float, t5cm: Float, t10cm: Float, t15cm: Float, t20cm: Float, t40cm: Float,
                       gtem: Float) extends EleX

final case class EOther(ymdhm: String, sta: Int, lat: Float, lon: Float, alt: Float,
                        vap: Float, dpt: Float, pre1h: Float, vism: Float, dddmax: Float, ffmax: Float,
                        winDAvg2mi: Float, winSAvg2mi: Float, //实际上是1m的
                        u: Float, v: Float) extends EleX

trait Cmiss[T <: EleX] {
  //type T <: EleX
  val ddd_max_nm = "WIN_D_INST"
  val ff_max_nm = "WIN_S_INST"

  def elex2map(a: Array[T]): TrieMap[Int, T] = {
    val trie = TrieMap[Int, T]()
    if (a.nonEmpty) {
      a.filter(f => StaDict.dictStaAll.keySet(f.sta))
        .par
        .foreach(f => trie += ((f.sta, f)))
    }
    trie
  }

  def mkURLStr(ymdhm_start: String, ymdhm_end: String = ""): String

  def json2EleX(j: JSONObject): EleX

  def getEleXArr(nm: String, ymdhm_start: String, ymdhm_end: String = ""): List[T] = {
    var nRetry = 0
    def getBuf(url : URL):BufferedSource = {
      val tbuf = Try(Source.fromURL(url, "UTF-8"))
      val buf: BufferedSource = tbuf match {
        case Success(v) =>  v
        case Failure(e) =>
          val msg = s"挂网了."
          logMsg(ymdhm_start, "2", "1", "F", s"$ymdhm_start# $msg")
          //sys.error(e.toString)
          null
      }
      buf
    }
    def getJsonArr(CmNm: String, ymdhm_start: String, ymdhm_end: String = ""): JSONArray = {
      val URLStr = mkURLStr(ymdhm_start, ymdhm_end)
      val url: URL = new URL(URLStr)
      val buf0 = getBuf(url)
      val buf = if (buf0 == null || buf0.isEmpty){
        val msg = s"第一次网络取数失败，再尝试一次."
        logMsg(ymdhm_start, "2", "1", "W", s"$ymdhm_start# $msg")
        if(buf0 != null)
          buf0.close()
        Thread.sleep(1000)
        getBuf(url)
      } else
        buf0
      try {
        if (buf == null || buf.isEmpty)
          sys.error("cimiss request fail !")
        else {
          val jsonTXT = buf.getLines().toArray.head
          val json = JSON.parseObject(jsonTXT)
          //val returnCode = json.getInteger("returnCode")
          val res = Try {
            json.get("DS").asInstanceOf[JSONArray] // JSON.parseArray(json.get("DS").toString)
          }
          res match {
            case Success(v) =>
              v
            case Failure(e) =>
              val msg = s"${e.toString}, Json解码失败，尝试重新网络取数."
              logMsg(ymdhm_start, "2", "1", "W", s"$ymdhm_start# $msg")
              if (nRetry > 10) {
                sys.error(e.toString)
              } else {
                nRetry += 1
                if(buf != null) buf.close()
                getJsonArr(CmNm, ymdhm_start, ymdhm_end)
              }
          }
        }
      } finally {
        if(buf != null)
          buf.close()
      }
    }

    //////////////////////////////////////////////////////////////////////
    val jsonArr: JSONArray = getJsonArr(nm, ymdhm_start, ymdhm_end)
    if (jsonArr != null && jsonArr.size > 0) {
      val numSta = jsonArr.size
      val arr = Array.ofDim[JSONObject](numSta)
      for (i <- (0 until numSta).par) {
        arr(i) = jsonArr.get(i).asInstanceOf[JSONObject]
      }

      val listElex = arr.par
        .map(jele => json2EleX(jele).asInstanceOf[T])
        .toList
        .filter(f => !(f == null))

      val msg = s"$nm $ymdhm_start--$ymdhm_end stations =${listElex.length}."
      logMsg(ymdhm_start, "2", "1", "O", s"$ymdhm_start# $msg")
      listElex
    } else {
      val msg = s"$nm cimiss 失败."
      logMsg(ymdhm_start, "2", "1", "F", s"$ymdhm_start# $msg")
      List[T]()
    }
  }
}
