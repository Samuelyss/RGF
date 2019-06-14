package stat

import common.BaseType.{EG, _}
import common.MyUtils._
import dict.{StaDict, StaNear}
import station.EleHOp
import station.cmiss.CimissLog

import scala.collection.concurrent.TrieMap
/**
  * 计算平流场。地面要素时间方向补缺。
  * Created by 何险峰，北京 on 16-7-15.
  */
case class Advect(ymdhm:String, mend : Array[StaEGMap], fore : Array[StaEGMap],real : Array[StaEGMap])
object Advect extends EleHOp{
  val idxSAvg: Int = getEleHIdx("WIN_S_Avg_2mi")
  val idxU : Int = getEleHIdx("U")
  val idxV : Int = getEleHIdx("V")
  def apply(ymdhm:String, egMapArr0 : Array[StaEGMap], egMapArrCur:Array[StaEGMap], seconds : Int):Advect={
    require(egMapArr0.length == numEle4Ground && egMapArrCur.length == numEle4Ground,"Advect fail!")
    showAbsent(ymdhm,egMapArrCur)
    val t0 = System.currentTimeMillis()
    val advCase = amend(ymdhm,egMapArr0,egMapArrCur,seconds)
    val dt = (System.currentTimeMillis() - t0)/1000
    val msg = s"平流外推花费时间$dt 秒"
    logMsg(ymdhm, "4","2", "I", s"$ymdhm# $msg")
    advCase
  }
  private def showAbsent(ymdhm:String,egMapArrCur:Array[StaEGMap]): Unit ={
    //对实况到达情况进行统计
    val arrive_arr = Array.fill[String](numEle4Ground)("")
    for (eleIdx <- 0 until numEle4Ground) {
      val cnt_real = egMapArrCur(eleIdx).count(f => ! f._2.ev.isNaN )
      val cnt_dict = StaDict.switchDict(eleIdx).size
      val absent =  cnt_dict - cnt_real
      val rate   = cnt_real / cnt_dict.toFloat
      val msg = f"$ymdhm,${EleNms(eleIdx)}%15s,$absent%6d,$rate%.2f,$cnt_real%6d,$cnt_dict%6d"
      arrive_arr(eleIdx) = msg
    }
    val msg = arrive_arr.mkString("\n")+"\n"
    CimissLog.logCimissWork(ymdhm, msg)
  }

  private def amend(ymdhm:String, egMapArr0 : Array[StaEGMap], egMapArrCur:Array[StaEGMap], seconds : Int):Advect={
    val egMapArrFore = forecast(egMapArr0,seconds)
    val mend = if (egMapArrCur.isEmpty || egMapArrCur.length <=10) egMapArrFore else {
      val egMapArr_new = Array.ofDim[StaEGMap](numEle4Ground)
      for (eleIdx <- 0 until numEle4Ground ) {
        val trie = TrieMap[Int,EG]()
        val sta_egMap0    = egMapArr0(eleIdx)
        val sta_egMapFore = egMapArrFore(eleIdx)
        val sta_egMapCur = egMapArrCur(eleIdx)
        StaDict.dictStaAll.par.foreach { s =>
          val sta = s._1
          if (sta_egMapFore.keySet(sta) && sta_egMap0.keySet(sta)) {
            val egFore: EG = sta_egMapFore(sta)
            val egCurOrFore: EG = if (!sta_egMapCur.keySet(sta)) egFore else sta_egMapCur(sta)
            trie += ((sta,egCurOrFore))
          }
        }
        egMapArr_new(eleIdx) = trie
      }
      egMapArr_new
    }
    Advect(ymdhm,mend,egMapArrFore,egMapArrCur)
  }

  def advect(eleNm : String, x:Float, speed:Float, g:Float, seconds:Int): Float ={
    def ddd360(d0 : Float):Float={
      val d1 = if ( d0 > 0.0)
        d0 % 360.0
      else
        360.0 + d0
      d1.toFloat
    }

    val sg = if (speed.isNaN || g.isNaN)
      0f
    else
      seconds *  g * speed / 111000f

    //val minu10 = seconds / 600f //10分钟为单位
    //val coe = if (minu10 < 1f) advRate else minu10 * advRate     //时间越长，平流贡献越大，平流作用小1个数量级

    val adv1 = if (!x.isNaN && x != 0f )
      eleNm match {
      case "PRE_1h"|"PRE_10m"|"PRE_5mprev"|"PRE_5mpost"|"VISM" =>
        logDiff(x, sg ).toFloat
      case "WIN_D_Avg_2mi" | "DDDMAX" =>
        val adv0 = x - sg
        ddd360(adv0)
      case "FFMAX" | "WIN_S_Avg_2mi" | "RHU"| "VAP" =>
        if (x < 0) Float.NaN else {
          val adv0 = x - sg
          if (adv0 <= 0)
            x
          else adv0
        }
      case "hsd" => x - sg
      case _ => x - sg
    } else x
    adv1
  }

  def forecast(egMapArr0: Array[StaEGMap],
               sta_egMap_SAvg:StaEGMap,
               seconds: Int,
               time_step : Int): Array[StaEGMap] = {
    //适用于地面和卫星数据
    if (seconds > 0) {
      val len = egMapArr0.length
      val egMapArr_new = Array.ofDim[StaEGMap](len)
      //val time_step = 10 * 60 // 10分钟时间步长
      for (eleIdx <- 0 until len) {
        val eleNm = len match {
          case 16 => "hsd"
          case _  => EleNms(eleIdx)
        }
        val sta_egMap0 = egMapArr0(eleIdx)
        val trie = TrieMap[Int,EG]()
        StaDict.dictStaAll.par.foreach {s =>
          val sta = s._1
          if (sta_egMap_SAvg.keySet(sta) && sta_egMap0.keySet(sta)) {
            val wind_speed = sta_egMap_SAvg(sta).ev

            val eg0 = sta_egMap0(sta)
            val e0 = eg0.ev
            val g0 = eg0.gv

            val e1 = advect(eleNm, e0, wind_speed, g0, time_step)

            val eg1 = EG(e1, g0)
            trie += ((sta,eg1))
          } else if (sta_egMap0.keySet(sta)) {
            val eg0 = sta_egMap0(sta)
            trie += ((sta,eg0))
          }
        }
        egMapArr_new(eleIdx) = trie
      }
      val sec = seconds - time_step
      val egMapArr_fore = mkGradeMap(egMapArr_new)
      val new_sta_egMap_SAvg = len match {
        case 16 => sta_egMap_SAvg
        case _  => egMapArr_fore(idxSAvg)
      }
      forecast(egMapArr_fore,new_sta_egMap_SAvg,sec,time_step)
    } else egMapArr0
  }

  def forecast(egMapArr0: Array[StaEGMap], seconds: Int): Array[StaEGMap] = {
    //仅仅适用于地面观测
    val sta_egMap_SAvg = egMapArr0(idxSAvg)
    val time_step = 10 * 60 // 10分钟时间步长
    forecast(egMapArr0,sta_egMap_SAvg,seconds,time_step)
  }

  def mkGradeMap(egMapArr_old: Array[StaEGMap]): Array[StaEGMap] = {
    val len = egMapArr_old.length
    val egMapArr = Array.ofDim[StaEGMap](len)
    for (eleIdx <- 0 until len) {
      val eleNm = len match {
        case 16 => "hsd"
        case _ => EleNms(eleIdx)
      }
      val sta_ele = TrieMap[Int,Float]()
      egMapArr_old(eleIdx).par.foreach(f => sta_ele += ((f._1, f._2.ev)))

      val dictNear = StaNear.switchNear(eleIdx, 0)
      val trie = TrieMap[Int,EG]()
      sta_ele.par.foreach { f =>
        val sta = f._1
        val e = sta_ele(sta)
        val grade: Float = calcu_chk_gradient(eleNm ,dictNear(sta), sta, sta_ele)
        trie += ((sta, EG(e,grade)))
      }
      egMapArr(eleIdx) = trie
    }
    egMapArr
  }
}
