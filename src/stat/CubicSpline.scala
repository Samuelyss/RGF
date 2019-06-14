package stat

import org.apache.commons.math3.analysis.polynomials.{PolynomialFunction,PolynomialSplineFunction}
/**
  * Created by 何险峰,北京 on 16-9-25.
  * http://stackoverflow.com/questions/14330737/spline-interpolation-performance-in-scala-vs-java
  */
object CubicSpline {

  def main(args: Array[String]): Unit = {
    val x = Array(0f, 1f, 2f, 3f)
    val y = Array(2f, 4f, 8f, 5f)
    val psf = cubicSpline(x,y)
    val a = 0
    println(psf.value(a))
  }

  def cubicSpline(x: Array[Float], y: Array[Float]) : PolynomialSplineFunction= {
    require(x.length == y.length,s"x.length=$x.length != y.length=$y.length error")
    // Number of intervals.  The number of data points is n + 1.
    val n = x.length - 1
    // Differences between knot points
    val h = Array.tabulate(n)(i => x(i+1) - x(i))
    val mu: Array[Float] = Array.ofDim[Float](n)
    val z: Array[Float] = Array.ofDim[Float](n+1)

    for ( i <- 1 until n) {
      val g = 2.0f * (x(i+1) - x(i-1)) - h(i-1) * mu(i-1)
      mu(i) = h(i) / g
      z(i) = (3.0f * (y(i+1) * h(i-1) - y(i) * (x(i+1) - x(i-1))+ y(i-1) * h(i)) /
        (h(i-1) * h(i)) - h(i-1) * z(i-1)) / g
    }
    // cubic spline coefficients --  b is linear, c quadratic, d is cubic (original y's are constants)
    val b: Array[Float] = Array.ofDim[Float](n)
    val c: Array[Float] = Array.ofDim[Float](n+1)
    val d: Array[Float] = Array.ofDim[Float](n)

    for (j <- n-1 to 0 by -1) {
      c(j) = z(j) - mu(j) * c(j + 1)
      b(j) = (y(j+1) - y(j)) / h(j) - h(j) * (c(j+1) + 2.0f * c(j)) / 3.0f
      d(j) = (c(j+1) - c(j)) / (3.0f * h(j))
    }
    val pfs = Array.tabulate(n){i =>
      val arr =  Array[Double](y(i), b(i), c(i), d(i))
      val pf = new PolynomialFunction(arr)
      pf
    }
    val xd = x.map(_.toDouble)
    val spf = new PolynomialSplineFunction(xd,pfs)
    spf
  }
}
