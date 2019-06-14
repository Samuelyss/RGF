package model.hour.ml

import common.BaseType.StaOneEleMap
import stat.StatisticsCase
import station.EleHOp

import scala.collection.SortedMap
import scala.collection.concurrent.TrieMap
import scala.util.Random
/**
  * 对不同要素进行重要性采样。在建立学习标签数据集中使用。
  * Created by 何险峰，北京 on 16-12-5.
  */
trait TSampImportance extends EleHOp{
  //val numRepeat: Int = 60              //样本重复数
  val ImportK: Int = 1000000           //重复样本开始号
  val minNumSamp=100


  def calcuTao(v : Float):Float={
    //对原值震荡10%
    val s = if (new Random().nextBoolean()) -1 else 1
    val r = new Random().nextFloat()
    val t = v * 0.02f
    t * r * s
  }

  def extSamp(samp: StaOneEleMap, statis: StatisticsCase, direct:Char,nRepeat : Int): StaOneEleMap = {
    //var buf = SortedMap[Int, Float]()
    def filtSamp(i:Int) = samp.filter { f =>
      val label = f._2
      if (direct == '>')
        label > (statis.mean + statis.max) / (i + 2)
      else
        label < (statis.mean + statis.min) / (i + 2)
    }

    val trie = TrieMap[Int,Float]()
    for (i <- 0 until nRepeat) {
      val key0 = ImportK * (i + 1)
      filtSamp(i).par.foreach { f =>
        val sta = f._1 + key0
        val label = f._2 + calcuTao(f._2)
        trie +=((sta, label))
      }
    }
    trie
  }
}
