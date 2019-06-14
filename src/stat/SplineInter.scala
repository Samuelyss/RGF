package stat

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector

/**
  * Created by hxf on 16-9-25.
  */
object SplineInter {
  def main(args: Array[String]): Unit = {
    val x = DenseVector(0.0, 1.0, 2.0, 3.0)
    val y = DenseVector(2.0, 4.0, 8.0, 5.0)
    val f = LinearInterpolator(x, y)
    val fit = f(2.5)
    println(fit)
  }
}
