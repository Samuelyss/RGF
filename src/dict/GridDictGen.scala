package dict

import common.BaseType.StaInfo


/**
  * 将格点场中每个格点编码为站号，形成格点场站点字典。
  * Created by 何险峰，维也纳 on 2017/3/4.
  */
object GridDictGen extends Dict{
  import grid.Dem._

  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    mkGridDict()
    val dt = (System.currentTimeMillis() - t1)/1000
    println("dt :",dt,"s")
  }

  def mkGridDict() : Unit={
    import common.MyConfig.dictGridFnm
    val buf = Array.ofDim[StaInfo](nLat,nLon)
    for (
      iLat <- (0 until nLat).par;
      jLon <- 0 until nLon;
      z = ij2z(iLat,jLon);
      lon = minLon + jLon * step;
      lat = maxLat - iLat * step;
      alt = dem(iLat)(jLon)
      if !alt.equals(Float.NaN)
    ) {
      buf(iLat)(jLon) = StaInfo(z,lat,lon,alt)
    }
    val seq = buf.flatten.filter(f => !(f == null))
    val fnm = getDictFnm(dictGridFnm)
    wrt(fnm,seq)
  }

  def wrt(fnm : String, staInfos : Array[StaInfo]): Unit ={
    val str = staInfos.map(f => s"${f.sta},${f.lat},${f.lon},${f.alt}").mkString("\n").trim
    common.MyUtils.wrt2txt(fnm,str)
  }

}
