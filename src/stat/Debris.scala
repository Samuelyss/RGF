package stat

/**
 * 目的：计算山地等价雨量
 * 参见<气象科技>，山地等价雨量
 * 何险峰 2014-02-17于成都
 */
object Debris {
  /**
   * 日衰减山地等价雨量
   */
  def hr(h: Option[Float], r0: Option[Float], rs: Array[Option[Float]]): Option[Float] = {
    val v = r0.getOrElse(0.0f) + 2.0 * sum(rs) * gauss(h)
    Some(v.toFloat)
  }
  /**
   * 小时无衰减山地等价雨量
   */
  def hr_now(h: Option[Float], r0: Option[Float], rs: Array[Option[Float]]): Option[Float] = {
    val v = r0.getOrElse(0.0f) + 2.0 * rs.foldLeft(0.0f)((r,c)=>r + c.getOrElse(0.0f)) * gauss(h)
    Some(v.toFloat)
  }

  /**
   * 含地形的雨量
   * @param h
   * @param r0
   * @return
   */
  def hr(h: Option[Float], r0: Option[Float]): Float ={
    val v = r0.getOrElse(0.0f) + r0.getOrElse(0.0f) * gauss(h)
    v.toFloat
  }

  def deltaRain(h: Float,r0 :Float) :Float={
    r0 * gauss(Some(h)).toFloat
  }

  def gauss(hh: Option[Float]): Double = {
    val mean = 1300
    val sgma = 400
    val h = hh.getOrElse(0.0f)
    //val v = if (h < 300 || h > 2500) { 0.0 } else {
    val v ={
      val d = (h - mean) / sgma
      val p = d * d / 2
      1 / (1 + Math.exp(p))
    }
    v
  }

  def sum(list: Array[Option[Float]]): Double = {
    var s = 0.0f
    var t = 1.0f
    for (i <- 0 to list.length - 1) {
      s += list(i).getOrElse(0.0f) * t
      t = t * 0.84f
    }
    s
  }
}