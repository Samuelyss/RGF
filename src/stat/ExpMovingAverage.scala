package stat

import common.BaseType.AF1

/**
  * Created by 何险峰，北京 on 16-6-14.
  */
object ExpMovingAverage {
  def main(args: Array[String]) {
    val xt = Array(1f,2f,3f,4f,0f,0f,0f,0f)
    val yt = expMovingAverage(0.618f,xt).mkString(",")
    println(yt)
  }

  def expMovingAverage(alpha: Float, xt: AF1): AF1 = {
    require(alpha > 0.0 && alpha <= 1.0, s"ExpMovingAverage found alpha = $alpha required > 0.0 and <= 1.0")
    if (xt.length <= 1) xt
    else {
      val alpha_1 = 1 - alpha
      var y = xt.head
      xt.map { x =>
        val z = x * alpha + y * alpha_1
        y = z
        z
      }
    }
  }
}