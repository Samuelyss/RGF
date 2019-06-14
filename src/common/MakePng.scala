package common

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import common.BaseType._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by hjn on 2015/7/2.
  */
object MakePng  {

  val Factor=1
  object ModelType extends Enumeration {
    type Model = Value
    val Ref, OneHour, FiveMinutes, Yellow, Observatory, Phase,Normal,dem = Value
  }

  val pngRoot = "C:\\Users\\hjn\\Desktop\\test\\png1\\"

  def main(args: Array[String]): Unit = {
    //testA("C:\\Users\\hjn\\Desktop\\mqpf_20151225_1030.data.hour.nc")
  }

//  def testAll(): Unit = {
//    val a = lstFile("C:\\Users\\hjn\\Desktop\\中气旋测试\\testNC\\20150825\\Z9371", "data.hour.nc")
//    for (i <- a.par)
//      testA(i.toString())
//  }
//
//  def testA(srcNC: String) {
//    val srcNCFile = new File(srcNC).getName
//    //val fnm = pngRoot + srcNCFile.split("[.]")(0) + ".png"
//    val fnm = s"C:/Users/hjn/Desktop/mqpf/" + srcNCFile.split("[.]")(0) + ".png"
//
//    val nfsrc = NetcdfFile.open(srcNC)
//    if (nfsrc.findVariable("time") != null) {
//      println(srcNC + "有外推")
//      val nRowsrc = nfsrc.readSection("lat").getSize().toInt
//      val nColsrc = nfsrc.readSection("lon").getSize().toInt
//      val nTimesrc = nfsrc.readSection("time").getSize().toInt
//      val lat0src = nfsrc.readSection("lat").getDouble(0)
//      val lon0src = nfsrc.readSection("lon").getDouble(0)
//      //val CompositeReflectivity = getMat3DS(nfsrc, "maxReflectiveTrack")
//      val maxReflectiveTrackB = getMat3DS(nfsrc, "qpf_ml")
//      val maxReflectiveTrackFactor = nfsrc.findVariable("qpf_ml").findAttribute("scale_factor").getNumericValue.doubleValue()
//      val maxReflectiveTrackOffset = nfsrc.findVariable("qpf_ml").findAttribute("add_offset").getNumericValue.doubleValue()
//      val maxReflectiveTrack = maxReflectiveTrackB.map(f => f.map(f => f.map(f => ((f * maxReflectiveTrackFactor + maxReflectiveTrackOffset) * 100).toShort)))
//      //      val maxReflectiveTrack = maxReflectiveTrackB.map(f => f.map(f => f.map(f => ((f).toShort))))
//
//      //println(matrixMax(maxReflectiveTrack(0)))
//
//      println("sdfs")
//      mkPng(maxReflectiveTrack(0).reverse, fnm, ModelType.FiveMinutes)
//    }
//    else {
//      println("无外推")
//      val nRowsrc = nfsrc.readSection("lat").getSize().toInt
//      val nColsrc = nfsrc.readSection("lon").getSize().toInt
//      val lat0src = nfsrc.readSection("lat").getDouble(0)
//      val lon0src = nfsrc.readSection("lon").getDouble(0)
//      val qpe = getMat2DB(nfsrc, "qpe_ml")
//      val maxReflective = getMat2DB(nfsrc, "maxReflective")
//      //println(matrixMax(maxReflective))
//      //mkPng(maxReflective,fnm,ModelType.Ref)
//    }


//  }

  def matrixMax(a: AS2) = {
    a.map(f => f.max).max
  }

  def batchPng(src: AS3, resolution: Int, dstRoot: String, intervalMinutes: Int, patten: String, standardCurrTime: DateTime, model: MakePng.ModelType.Model): Unit = {

    for (i <- (0 until src.length).par) {
      println(s"制作第${i + 1}个预报图...")
      val m =src(i) //LatLon2Mercator.l2m(src(i), resolution)
      val fnmFmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
      val j = if (intervalMinutes == 5) i else i + 1
      val fnmTimeStr = fnmFmt.print(standardCurrTime.plusMinutes(j * intervalMinutes))
      val fldFmt = DateTimeFormat.forPattern("yyyyMMdd")
      val fldTimeStr = fldFmt.print(standardCurrTime.plusMinutes(j * intervalMinutes))
      val pngName = s"${dstRoot}${fldTimeStr}/${patten}${fnmTimeStr}.png"
      val tempPngName = s"${pngName}.tmp"
      MakePng.mkPng(m, tempPngName, model)
      //chName(tempPngName, pngName)
    }

  }

  def mkPng(arr: AS2, fnm: String, model: ModelType.Model): Unit = {
    new File(fnm).mkdirs()
    val y = arr.length
    val x = arr(0).length
    val d2 = Array.fill[Int](y, x)(0)
    val bufferedImage = new BufferedImage(x, y, BufferedImage.TYPE_4BYTE_ABGR)
    for (i <- (0 until y).par;
         j <- 0 until x)
      d2(i)(j) = value2ARGB(arr(i)(j), model)
    val d1 = d2.flatten
    bufferedImage.setRGB(0, 0, x, y, d1, 0, x) //设置RGB
    ImageIO.write(bufferedImage, "png", new File(fnm)) //写图片
  }

  def value2ARGB(a: Short, model: ModelType.Model): Int = {
    //    def value2ARGBref(a: Short, factor: Double): Int = {
    //      val v = a * factor
    //      val argb =
    //        if(v >= 5 && v < 10)
    //          rgb2Int(100, 230,224,236)
    //        else if (v >= 10 && v < 15)
    //          rgb2Int(100, 8, 161, 244)
    //        else if (v >= 15 && v < 20)
    //          rgb2Int(100, 0, 236, 236)
    //        else if (v >= 20 && v < 25)
    //          rgb2Int(100, 0, 216, 0)
    //        else if (v >= 25 && v < 30)
    //          rgb2Int(100, 0, 144, 0)
    //        else if (v >= 30 && v < 35)
    //          rgb2Int(100, 255, 255, 0)
    //        else if (v >= 35 && v < 40)
    //          rgb2Int(100, 231, 192, 0)
    //        else if (v >= 40 && v < 45)
    //          rgb2Int(100, 255, 144, 0)
    //        else if (v >= 45 && v < 50)
    //          rgb2Int(100, 255, 0, 0)
    //        else if (v >= 50 && v < 55)
    //          rgb2Int(100, 214, 0, 0)
    //        else if (v >= 55 && v < 60)
    //          rgb2Int(100, 192, 0, 0)
    //        else if (v >= 60 && v < 65)
    //          rgb2Int(100, 255, 0, 240)
    //        else if (v >= 65 && v < 70)
    //          rgb2Int(100, 150, 0, 180)
    //        else if (v >= 70 && v <= 100)
    //          rgb2Int(100, 173, 144, 240)
    //        else
    //          rgb2Int(0, 255, 255, 255)
    //      argb
    //    }
    def value2ARGBref(a: Short, factor: Double): Int = {
      if (!a.equals(MISSING_SHORT)) {
        val v = a * factor
        val argb =
        //        if(v >= -5 && v < 0)
        //          rgb2Int(255, 74,234,230)
        //        else if (v >= 0 && v < 5)
        //          rgb2Int(255, 40, 152, 196)
        //          if (v >= 5 && v < 10)
        //            rgb2Int(255, 40, 90, 220)
          if (v >= 10 && v < 15)
            rgb2Int(255, 32, 24, 172)
          else if (v >= 15 && v < 20)
            rgb2Int(255, 100, 218, 80)
          else if (v >= 20 && v < 25)
            rgb2Int(255, 42, 182, 46)
          else if (v >= 25 && v < 30)
            rgb2Int(255, 8, 118, 36)
          else if (v >= 30 && v < 35)
            rgb2Int(255, 222, 184, 96)
          else if (v >= 35 && v < 40)
            rgb2Int(255, 222, 222, 0)
          else if (v >= 40 && v < 45)
            rgb2Int(255, 254, 254, 0)
          else if (v >= 45 && v < 50)
            rgb2Int(255, 218, 0, 0)
          else if (v >= 50 && v < 55)
            rgb2Int(255, 198, 0, 0)
          else if (v >= 55 && v < 60)
            rgb2Int(255, 254, 0, 0)
          else if (v >= 60 && v < 65)
            rgb2Int(255, 254, 0, 254)
          else if (v >= 65 && v < 100)
            rgb2Int(255, 254, 254, 254)
          else
            rgb2Int(0, 255, 255, 255)
        argb
      }
      else
        rgb2Int(30, 0, 0, 0)
    }
    def value2Range(a: Short): Int = {
      val argb =
        if (a == MISSING_SHORT)
          rgb2Int(20, 0, 0, 0)
        else
          rgb2Int(0, 0, 0, 0)
      argb
    }
    def value2Observatory(a: Short, factor: Double): Int = {
      val v = (a * factor).toInt
      val argb =
        if (v == 0)
          rgb2Int(0, 255, 255, 255)
        else
          rgb2Int(255, v, v, v)
      argb
    }
    def value2Yellow(a: Short, factor: Double): Int = {
      val v = (a * factor).toInt
      val argb =
        if (v == 0)
          rgb2Int(0, 255, 255, 255)
        else
          rgb2Int(255, 175, 132, 0)
      argb
    }
    def value2ARGB1h(a: Short, factor: Double): Int = {
      if (!a.equals(MISSING_SHORT)) {
        val v = a * factor
        val argb =
          if (v >= 1 && v < 2)
            rgb2Int(255, 159, 240, 135)
          else if (v >= 2 && v < 4)
            rgb2Int(255, 55, 163, 2)
          else if (v >= 4 && v < 6)
            rgb2Int(255, 96, 185, 253)
          else if (v >= 6 && v < 8)
            rgb2Int(255, 2, 4, 237)
          else if (v >= 8 && v < 10)
            rgb2Int(255, 0, 114, 78)
          else if (v >= 10 && v < 20)
            rgb2Int(255, 253, 0, 255)
          else if (v >= 20 && v < 50)
            rgb2Int(255, 231, 75, 1)
          else if (v >= 50)
            rgb2Int(255, 112, 3, 0)
          else
            rgb2Int(0, 255, 255, 255)
        argb
      }
      else{
        rgb2Int(30, 0, 0, 0)
      }
    }

    //      def value2ARGB5m(a: Short, factor: Double): Int = {
    //        if (!a.equals(MISSING_SHORT)) {
    //          val v = a * factor
    //          val argb =
    //            if (v > 0 && v < 1)
    //              rgb2Int(255, 229, 115, 30)
    //            else if (v >= 1 && v < 2.5)
    //              rgb2Int(255, 240, 234, 23)
    //            else if (v >= 2.5 && v < 8)
    //              rgb2Int(255, 0, 196, 71)
    //            else if (v >= 8 && v < 16)
    //              rgb2Int(255, 22, 82, 198)
    //            else if (v >= 16)
    //              rgb2Int(255, 125, 104, 174)
    //            else
    //              rgb2Int(0, 255, 255, 255)
    //          argb
    //        }
    //        else
    //          rgb2Int(30, 0, 0, 0)
    //      }
    def value2ARGB5m(a: Short, factor: Double): Int = {
      if (!a.equals(MISSING_SHORT)) {
        val v = a * factor
        val argb =
          if (v > 0 && v < 2)
            rgb2Int(255, 229, 115, 30)
          else if (v >= 2 && v < 5)
            rgb2Int(255, 240, 234, 23)
          else if (v >= 5 && v < 16)
            rgb2Int(255, 0, 196, 71)
          else if (v >= 16)
            rgb2Int(255, 22, 82, 198)
          else
            rgb2Int(0, 255, 255, 255)
        argb
      }
      else
        rgb2Int(30, 0, 0, 0)
    }
    def value2Phase(v: Short): Int = {
      val argb =
        if (v.equals(0.toShort))
          rgb2Int(255, 255, 0, 0)
        else if (v.equals(1.toShort))
          rgb2Int(255, 0, 255, 0)
        else if (v.equals(2.toShort))
          rgb2Int(255, 0, 0, 255)
        else
          rgb2Int(0, 255, 255, 255)
      argb
    }
    def value2Normal(v: Short): Int ={
      val argb =
        if (v>=0 && v<=255)
          rgb2Int(255,v, v, v)
        else
          rgb2Int(0, 255, 255, 255)
      argb
    }
    def value2DEM(v: Short): Int ={
      val argb =
        if(v >= -300 && v < 1000)
          rgb2Int(255, 74,234,230)
        else if (v >= 1000 && v < 2000)
          rgb2Int(255, 40, 152, 196)
        else if (v >= 2000 && v < 3000)
        rgb2Int(255, 40, 90, 220)
        else if (v >= 3000 && v < 4000)
          rgb2Int(255, 32, 24, 172)
        else if (v >= 4000 && v < 5000)
          rgb2Int(255, 100, 218, 80)
        else if (v >= 5000 && v < 6000)
          rgb2Int(255, 42, 182, 46)
        else if (v >= 6000 && v < 7000)
          rgb2Int(255, 8, 118, 36)
        else if (v >= 7000 && v < 8000)
          rgb2Int(255, 222, 184, 96)
//        else if (v >= 35 && v < 40)
//          rgb2Int(255, 222, 222, 0)
//        else if (v >= 40 && v < 45)
//          rgb2Int(255, 254, 254, 0)
//        else if (v >= 45 && v < 50)
//          rgb2Int(255, 218, 0, 0)
//        else if (v >= 50 && v < 55)
//          rgb2Int(255, 198, 0, 0)
//        else if (v >= 55 && v < 60)
//          rgb2Int(255, 254, 0, 0)
//        else if (v >= 60 && v < 65)
//          rgb2Int(255, 254, 0, 254)
//        else if (v >= 65 && v < 100)
//          rgb2Int(255, 254, 254, 254)
        else
          rgb2Int(0, 255, 255, 255)
      argb
    }
    if (model == ModelType.FiveMinutes)
      value2ARGB5m(a, Factor)
    else if (model == ModelType.OneHour)
      value2ARGB1h(a, Factor)
    else if (model == ModelType.Ref)
      value2ARGBref(a, Factor)
    else if (model == ModelType.Observatory)
      value2Observatory(a, Factor)
    else if (model == ModelType.Yellow)
      value2Yellow(a, 1f)
    else if (model == ModelType.Phase)
      value2Phase(a)
    else if (model == ModelType.Normal)
      value2Normal(a)
    else if (model == ModelType.dem)
    value2DEM(a)
    else Int.MinValue

  }

  private def rgb2Int(a: Int, r: Int, g: Int, b: Int): Int =
  {
    a << 24 | r << 16 | g << 8 | b
  }
}

