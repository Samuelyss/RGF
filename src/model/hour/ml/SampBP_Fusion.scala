package model.hour.ml

import common.BaseType.StaElesMap

/**
  * 机器迭代学习后，会对信任传播结果产生融合(fusion)后结果
  * Created by 何险峰，北京 on 16-12-6.
  */
case class SampBP_Fusion(ymdhm : String,fusion : StaElesMap,sampBPPrev : TSamp, dSrc :String) extends TSamp{
  def getSta_af1 : Array[StaElesMap]={
    Array(fusion,sampBPPrev.sta_af1_src_real)
  }
}
