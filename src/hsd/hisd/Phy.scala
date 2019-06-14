package hsd.hisd

import scala.math._

/**
  * Created by 何险峰，成都 on 2015/10/3.
  */
case class Phy(fnm: String, radiance: Array[Array[Float]], bandNo: Int)

//case class ProfilePhys(pf: Profile, phys: Array[Phy])

object Phy  {
  import DataType._
  type LS = List[String]
  type AH1 = Array[Head]

  def apply(profile: Profile): Phy = {
    sat2phy(profile: Profile)
  }

  private def sat2phy(pf: Profile): Phy = {
    val v = sat2latlon(pf)
    val head = pf.heads(0)
    val calib = head.calib
    val bandNo = head.calib.bandNo
    val gain_cnt2rad = calib.gain_cnt2rad.toFloat
    val cnst_cnt2rad = calib.cnst_cnt2rad.toFloat
    val satName = head.basic.satName
    val radiance1: AF2 = v.map(f => f.map(m => m * gain_cnt2rad + cnst_cnt2rad))
    val radiance2 : AF2 =
    /*
    1    : 兰光反射率,输出值范围[0,1]
    2    : 绿光反射率,输出值范围[0,1]
    3    : 红光反射率,输出值范围[0,1]
    4--6 : 近红外反射率,输出值范围[0,1]
    7--16: 红外辐射亮温,输出值范围[100 -- 350k]
     */
    if (bandNo >= 1 && bandNo <= 6)
      radiance1.map(f => f.map(g => g * calib.rad2albedo.toFloat))
    else //if (bandNo >= 7 && bandNo <= 16)
      hisd_radiance_to_tbb(head, radiance1)
    /*
    band 1-6  输出值范围[0,1]
    band 7-16 输出值范围[100 -- 350k]
    val minT = radiance2.flatten.min
    val maxT = radiance2.flatten.max
    println(s"min=${minT} max=${maxT}")
    */
    Phy(head.fnm, radiance2, bandNo)
  }

  // #按照行列号读取数据
  private def sat2latlon(pf: Profile): AF2 = {
    val rows = pf.pixLin.lin.length
    val cols = pf.pixLin.lin(0).length
    val v = Array.fill[Float](rows, cols)(-99990.0f)
    for (y <- (0 until rows);
         x <- (0 until cols);
         r = pf.region(y)(x);
         l = pf.pixLin.lin(y)(x);
         p = pf.pixLin.pix(y)(x);
         head = pf.heads(r)) {
      import head._
      val R_C = navcorr.RoCenterColumn
      val R_L = navcorr.RoCenterLine
      val R_A = navcorr.RoCorrection / 1000.0f / 1000.0f
      val l1 = (l - R_L) * cos(R_A) - (p - R_C) * sin(R_A) + R_L
      val p1 = (p - R_C) * cos(R_A) + (l - R_L) * sin(R_A) + R_C
      val l2 = (round(l1) - seg.strLineNo).toShort
      val p2 = (round(p1) - 1).toShort
      val dat: Short = cdata(l2)(p2)
      v(y)(x) = if (dat < 0.0) Float.NaN else dat.toFloat
    }
    v
  }

  //#将辐射值转换为亮温 (7-16 通道)
  private def hisd_radiance_to_tbb(head: Head, radiance0: AF2): AF2 = {
    import head.calib._
    val lmbd = waveLen / 1000000.0f
    val radiance: AF2 = radiance0.map(f => f.map(g => g * 1000000.0f))
    val planck_c1 = 2.0 * planckConst * pow(lightSpeed, 2.0) / pow(lmbd, 5.0)
    val planck_c2 = planckConst * lightSpeed / bolzConst / lmbd
    val tbb: AF2 = radiance.map(f =>
      f.map { g =>
        if (g > 0) {
          val effective_temperature = planck_c2 / log((planck_c1 / g) + 1.0)
          rad2btp_c0 + rad2btp_c1 * effective_temperature + rad2btp_c2 * pow(effective_temperature, 2.0)
        } else Double.NaN
      }.map(_.toFloat)
    )
    tbb
  }
}