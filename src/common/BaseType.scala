package common
import scala.collection.SortedMap
import scala.collection.concurrent._

/**
  * Created by 何险峰，北京 on 15-12-9.
  */
object BaseType {
  case class StaInfo(sta: Int, lat: Float, lon: Float, alt: Float)
  case class StaWD(sta: Int, w: Float,d:Float,dz : Float)
  case class EG(ev : Float,gv:Float)
  case class Latitude(minLat: Float, maxLat: Float, step: Float, nLat: Int, lats: SMFI)
  case class Longitude(minLon: Float, maxLon: Float, step: Float, nLon: Int, lons: SMFI)

  val MissingFloat: Float = Float.NaN
  val MISSING_SHORT:Short = -9999
  val MissingEle = 999990.0f

  type AI1 = Array[Int]
  type AF1 = Array[Float]
  type AF2 = Array[AF1]
  type AS1 = Array[Short]
  type AS2 = Array[AS1]
  type AS3 = Array[AS2]
  type StaElesMap = TrieMap[Int, AF1]
  type StaOneEleMap = TrieMap[Int, Float]
  type StaEGMap   = TrieMap[Int, EG]
  type SMFI       = SortedMap[Float, Int]
  type SMDI       = SortedMap[Double, Int]

  type StawdArr   = Array[StaWD]
  type LLPair     = (Float, Float)
  type MMPair     = (Float, Float)
  type SeqLLPair  = Seq[LLPair]
  type StaMap     = TrieMap[Int, StaInfo]
  type StaWDMap   = TrieMap[Int, StawdArr]
  type Tup3       = (Int, Double, AF1)
  type SeqTup3    = Seq[Tup3]
  type ArrSeqTup3 = Array[SeqTup3]
}
