package station.hsd

import common.BaseType.{AF1, StaElesMap}
import common.MyUtils.{arrStaEg2staAf1, logMsg}
import common.TimeStaAf1Cach
import common.TimeTransform.prevYmdhm_minute
import grid.hsd.HsdGd


object StaHsdExt extends StaHsd8{
  val buf = TimeStaAf1Cach
  def main(args: Array[String]) {
    val eleMapArr = this ("201609070100")
    println(s"站点数：${eleMapArr.size}")
  }

  def apply(ymdhm: String) : StaElesMap = {
    val sta_af1New = mkContainTimeDiff(ymdhm)
    sta_af1New
  }

  private def mkContainTimeDiff(ymdhm:String): StaElesMap={
    val prevYmdhm = prevYmdhm_minute(ymdhm,10)
    val sta_af1Prev = getSta_hsdAf1(prevYmdhm)
    val sta_af1Cur = getSta_hsdAf1(ymdhm)
    val sta_af1New =
      if (sta_af1Prev == null && sta_af1Cur == null)
        null
      else if (sta_af1Prev == null)
        sta_af1Cur
      else if (sta_af1Cur == null)
        sta_af1Prev
      else sta_af1Cur.map{f =>
        val sta = f._1
        val af1Cur : AF1 = f._2
        val af1Prev : AF1 = sta_af1Prev(sta)
        val diff : AF1 = Array.tabulate[Float](HsdGd.nBand){i=> af1Cur(i) - af1Prev(i) }
        val join = af1Cur ++ diff
        sta -> join
      }
    sta_af1New
  }
  private def getSta_hsdAf1(ymdhm : String): StaElesMap ={
    val sta_hsdAf1 : StaElesMap= if (buf.contains(ymdhm.toLong)) {
      buf.get(ymdhm.toLong)
    } else {
      val sta_egMapArr1 = mkHsdStaEGMapArr(ymdhm)
      if (sta_egMapArr1 == null) {
        val msg = s"缓存卫星数据失败"
        logMsg(ymdhm, "1", "1", "W", s"$ymdhm# $msg")
        null
      } else {
        //将观测值，梯度值用一维数组表示
        val af1 = arrStaEg2staAf1(sta_egMapArr1, HsdGd.nBand)
        buf.keep(ymdhm.toLong, af1)
        val msg = s"缓存卫星数据成功"
        logMsg(ymdhm, "1", "1", "O", s"$ymdhm# $msg")
        af1
      }
    }
    sta_hsdAf1
  }
}
