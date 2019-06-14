package common

import common.BaseType.{ArrSeqTup3, SeqTup3, StaElesMap}
import common.MyConfig.nFit
import scala.collection.concurrent._
/**
  * Created by 何险峰，成都  on 2017/9/16.
  */
trait TrieMapBuf[T] {
  val cach = new TrieMap[Long,T]()
  val bufLen = 3

  def toLong(ymdhm:Long):Long={
    ymdhm.toLong
  }

  def contains(ymdhm:Long):Boolean={
    cach.contains(ymdhm)
  }

  def get(ymdhm:Long):T={
    cach(ymdhm)
  }

  def vals:Seq[T]={
    cach.values.toSeq
  }

  def trim{
    while(cach.size > bufLen) {
      val mn = cach.keySet.min
      cach -=(mn)
    }
  }

  def keep(ymdhm:Long,v : T) {
    if (cach.nonEmpty && ymdhm < cach.keySet.min) {
      cach.clear
      cach +=((ymdhm,v))
    } else if (!cach.contains(ymdhm)) {
      cach +=((ymdhm,v))
      trim
    }
  }
}

/*
在卫星StaHsdExt中应用
 */
object TimeStaAf1Cach extends TrieMapBuf[StaElesMap]{
  override def keep(ymdhm:Long,v : StaElesMap): Unit = {
    if (v !=null && v.nonEmpty)
      super.keep(ymdhm,v)
  }
  def main(args: Array[String]): Unit = {
    val buf = TimeStaAf1Cach

    buf.keep(201709060140L, null)
    println(buf.cach)
    buf.keep(201709060000L, null)
    println(buf.cach)

    buf.keep(201709060120L, null)
    println(buf.cach)

    buf.keep(201709060100L, null)
    println(buf.cach)
  }
}

object TimeDFLearnBuf extends TrieMapBuf[ArrSeqTup3] {
  private var curYmdhm : String=""
  private val arrSeqTup3Buf = Array.ofDim[SeqTup3](nFit)

  override def keep(ymdhm:Long,v : ArrSeqTup3): Unit = {
    if (v !=null && v.nonEmpty)
      super.keep(ymdhm,v)
  }

  def refresh(ymdhm : String,fitEleIdx : Int, seqTup3 : SeqTup3): ArrSeqTup3 ={
    if (curYmdhm == ymdhm) {
      arrSeqTup3Buf(fitEleIdx)=seqTup3
    } else {
      curYmdhm = ymdhm
      for (i <- arrSeqTup3Buf.indices) arrSeqTup3Buf(i) = null
      arrSeqTup3Buf(fitEleIdx) = seqTup3
    }
    arrSeqTup3Buf
  }
}
