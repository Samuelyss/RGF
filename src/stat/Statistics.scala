package stat

/**
  * Created by 何险峰,北京 on 15-7-1.
  */
case class StatisticsCase(max: Float,min: Float,sum : Float,count: Long,mean: Float,variance: Float,stdev: Float){
  def toStd(e : Float):Float={
    if (e.equals(Float.NaN)) mean else (e - mean) / stdev
  }
  def toOrg(x : Float):Float={
    x * stdev + mean
  }
  def boundCtl(c:Float):Float={
    if(c.equals(Float.NaN)) Float.NaN else if (c > max ) max else if (c<min) min else c
  }
  override def toString:String={
    s"min=${min},max=${max},count=${count},mean=${mean},sum=${sum},stddev=${stdev}"
  }
}
object Statistics {
  def main(args: Array[String]) {
    val a=Array(1f,2f,3f,4,5f)
    val s=this(a)
    val b = a.map(f => s.toStd(f))
    println(b.mkString(","))
    println(s.toString)
  }
  def apply(values: Array[Float]):StatisticsCase={
    val max: Float = values.max
    val min: Float = values.min
    val sum : Float = values.sum
    val count: Long = values.length
    val mean: Float = sum / count
    val va  : Float = values.foldLeft(0.0f)((r,c) => r + sqr(c - mean)) / count
    val stdev: Float = math.sqrt(va).toFloat
    StatisticsCase(max,min,sum,count,mean,va,stdev)
  }
  def toStand(values: Array[Float]):Array[Float]={
    val statis = this(values)
    values.map(f => statis.toStd(f))
  }
  def sqr(v : Float) = v * v
}
