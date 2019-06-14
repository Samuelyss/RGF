package hsd.nc

import hsd.hisd.Phy

/**
 * Created by 何险峰，成都 on 2015/10/4.
 */
class  SingleBandNC(val ofn: String, val ymdhm: String, val phys : Array[Phy]) extends HimaNc {
  def mkNc4: Unit = {
    val phy = phys(0)
    // println(s"输出文件:${ofn}")
    val radiance = phy.radiance
    val bandNo = phy.bandNo
    val vv = if (bandNo <=6) mkAlbedo(radiance,bandNo) else mkTemp(radiance,bandNo)
    //////////////////////////////////////////////////////////
    writer.create()
    writer.write(time, timeVal)
    writer.write(latitude, latVal)
    writer.write(longitude, lonVal)
    writer.write(vv.var0, vv.val0)
    writer.close()
  }
}
