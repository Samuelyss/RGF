package hsd.nc

import hsd.hisd.Phy
import hsd.hisd.HsdConfig._
/**
  * Created by 何险峰，北京 on 15-12-1.
  */
class MutiBandNc(val ofn: String, val ymdhm: String, val phys: Array[Phy]) extends HimaNc {

  def mkNc4(): Unit = {
   val vvPairs = Array.ofDim[VarValPair](nBand)
    for (i<-0 until nBand){
      val phy = phys(i)
      val radiance = phy.radiance
      val bandNo = phy.bandNo
      vvPairs(i) = if (bandNo <=6) mkAlbedo(radiance,bandNo) else mkTemp(radiance,bandNo)
    }
    //////////////////////////////////////////////////////////
    writer.create()
    writer.write(time, timeVal)
    writer.write(latitude, latVal)
    writer.write(longitude, lonVal)
    for (i<-0 until nBand) writer.write(vvPairs(i).var0, vvPairs(i).val0)
    writer.close()
  }
}
