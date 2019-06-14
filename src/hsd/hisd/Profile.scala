package hsd.hisd

import java.io.File
import HsdConfig._
/**
  * 一幅特定通道完整卫星图，由一组(1..10)扫描线文件构成。
  * Profile将一幅完整卫星图矩阵:
  * 1.读入每一组扫面线文件；
  * 2.对矩阵填入组编号，形成图的轮廓。
  *  Created by 何险峰，成都 on 2015/10/3.
 */
case class Profile(heads : Array[Head],pixLin:Prj,region:Array[Array[Short]])

object Profile{
  import DataType._

  import sys.process._

  def apply(hsd_ftpDir:String,ymd : String,hhmm : String,band : Int,needRmDatFnm : Boolean,ramdisk:String):Profile={
    val swathFnms :AStr1 = wget_bunzip(hsd_ftpDir,ymd,hhmm,band,ramdisk)
    mkProfile(swathFnms,needRmDatFnm)
  }
  def apply(swathFnms:AStr1):Profile={
    val needRmDatFnm : Boolean=false
    mkProfile(swathFnms,needRmDatFnm)
  }

  //建立一个通道，所有扫描文件配置背景
  private def mkProfile(swathFnms : AStr1,needRmDatFnm : Boolean):Profile={

    require(swathFnms.length == numSwath)
    val heads = Array.ofDim[Head](numSwath)
    val startLine = Array.ofDim[Int](numSwath)
    val endLine = Array.ofDim[Int](numSwath)
    //val nr  = Array.fill[Short](numFile)(-1)
    for(f <- (0 until numSwath).par
        if swathFnms(f).length > 0){
      val h = Head(swathFnms(f))
      heads(f) = h
      val s = h.seg.strLineNo
      val e = s + h.data.nLin - 1
      //nr(f) = f.toShort
      startLine(f) = s
      endLine(f) = e
    }
    val pixLin = Prj(heads(0))
    val region = mkRegion(pixLin.lin, startLine,endLine)

    if (needRmDatFnm) s"rm ${swathFnms.mkString(" ")}" !


     //"ls" #| "grep HS_H08_" #&& Seq("sh","-c","rm HS_H08_*") !

    Profile(heads,pixLin,region)
  }

  private def mkRegion(lin: AS2,startLine : AI1,endLine:AI1): AS2 = {
    require(startLine.length >= 1, "Defined region is out of data spatial range")
    val rows = lin.length
    val cols = lin(0).length
    val linRgn = Array.fill[Short](rows,cols)(-1)
    for (i <- (0 until rows).par;
         j <- (0 until cols).par) {
      linRgn(i)(j) = getRgn(lin(i)(j),startLine,endLine)
    }
//    Utils.save2csv("linRgn.csv",linRgn)
    linRgn
  }
  private def getRgn(alin : Short,startLine : AI1,endLine:AI1):Short={
    var g:Short = -1
    for (r<-0 until numSwath; if g == -1){
      val s = startLine(r)
      val e = endLine(r)
      if (g == -1 && alin>=s && alin<=e) {
        g = r.toShort
      }
    }
    g
  }

  private def wget_bunzip(hsd_ftpDir:String,ymd : String,hhmm : String,band : Int,ramdisk:String):AStr1={
    //_Band_Resolution
    //val b = band
    val i = band -1
    val _B_R =s"_B${bands(i).n}_FLDK_R${bands(i).res}"
    //_swath
    val _S = Range(1,numSwath).inclusive.map(swath => if (swath<10) s"_S0${swath}10" else s"_S${swath}10")
    //val _S = Range(1,numSwath).inclusive.map(swath => "%02d".format(swath))
    /*
    ftp://pub_data:xxshj@10.1.72.41//SATE/Himawari-8/fulldisk/HSD/20151126/HS_H08_20151126_0000_B01_FLDK_R10_S0110.DAT.bz2
     */
    import sys.process._
    val swathFnms = Array.fill[String](_S.length)("")
    for (i <- (0 until numSwath).par ){
      //val res = Profile.ress(i)
      val bz2Fnm: String = s"HS_H08_${ymd}_$hhmm${_B_R}${_S(i)}.DAT.bz2"
      val swathFnm = s"$ramdisk/HS_H08_${ymd}_$hhmm${_B_R}${_S(i)}.DAT"

      val ftp: String = s"$hsd_ftpDir/$ymd/$bz2Fnm"

      val wgetResult = s"wget -c -q -nc $ftp -P $ramdisk" !

      Thread.sleep(50)

      //bunzip2 将bz2文件解压为datFnm后，会自动删除bz2Fnm
      val tmpFnm = s"$ramdisk/$bz2Fnm"

      if(wgetResult == 0 && new File(tmpFnm).exists){

        s"bunzip2 -qdf $tmpFnm" !

      }
      val exists = new File(swathFnm).exists
      //println(s"wgetResult : ${wgetResult} , $swathFnm exists= $exists")

      swathFnms(i) = if(exists) swathFnm else ""
    }
    swathFnms
  }
}