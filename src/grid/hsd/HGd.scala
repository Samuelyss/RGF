package grid.hsd

import common.BaseType._
import common.MyUtils._
import ucar.ma2.ArrayFloat
import ucar.nc2.dataset.NetcdfDataset

/**
  * Created by 何险峰，北京  on 16-6-28.
  */
trait HGd {
  val LatNm = "lat"
  val LonNm = "lon"
  val nBand = 16
  def getF2I(nc: NetcdfDataset, nm: String): SMFI = {
    val v = nc.findVariable(nm)
    val arr = v.read.asInstanceOf[ArrayFloat.D1]
    val dim1 = arr.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    arr_idx(dim1)
  }
  def getNcLat(nc: NetcdfDataset): SMFI = {
    getF2I(nc, LatNm)
  }
  def getNcLon(nc: NetcdfDataset): SMFI = {
    getF2I(nc, LonNm)
  }
  def var2AF2(nc: NetcdfDataset, varNm: String): AF2 ={
    val dim = nc.getDimensions
    val nDim = dim.size()
    val (nLat,nLon) = nDim match{
      case 2 => (dim.get(0).getLength,dim.get(1).getLength)
      case 3 => (dim.get(1).getLength,dim.get(2).getLength)
    }
    if (nDim == 3)
      var2AF2_dim3(nc,varNm,nLat,nLon)
    else
      var2AF2_dim2(nc,varNm,nLat,nLon)
  }
  def var2AF2_dim3(nc: NetcdfDataset, varNm: String,nLat : Int,nLon:Int): AF2 = {
    val v = nc.findVariable(varNm)
    val vb3 = v.read.asInstanceOf[ArrayFloat.D3]
    val mat = Array.fill[Float](nLat, nLon)(Float.NaN)
    for (i <- (0 until nLat).par;
         j <- (0 until nLon).par) {
      val vv = vb3.get(0,i, j)
      if (!vv.isNaN || vv > -999) {
        mat(i)(j) = vv
      }
    }
    mat
  }
  def var2AF2_dim2(nc: NetcdfDataset, varNm: String,nLat : Int,nLon:Int): AF2 = {
    val v = nc.findVariable(varNm)
    val vb2 = v.read.asInstanceOf[ArrayFloat.D2]
    val mat = Array.fill[Float](nLat, nLon)(Float.NaN)
    for (i <- (0 until nLat).par;
         j <- (0 until nLon).par) {
      val vv = vb2.get(i, j)
      if (!vv.isNaN || vv > -999) {
        mat(i)(j) = vv
      }
    }
    mat
  }
}
